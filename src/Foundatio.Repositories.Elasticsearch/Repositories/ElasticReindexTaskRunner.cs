using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Jobs;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Extensions;
using Foundatio.Serializer;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Foundatio.Repositories.Elasticsearch;

internal sealed class ElasticReindexTaskRunner
{
    private const int MaxStatusFailures = 10;
    private readonly ElasticsearchClient _client;
    private readonly ITextSerializer _serializer;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger _logger;

    public ElasticReindexTaskRunner(ElasticsearchClient client, ITextSerializer serializer, TimeProvider timeProvider, ILogger? logger = null)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        _timeProvider = timeProvider ?? TimeProvider.System;
        _logger = logger ?? NullLogger.Instance;
    }

    public async Task<ElasticReindexTaskResult> RunCompatibilityReindexAsync(
        string sourceIndex,
        string targetIndex,
        int? batchSize,
        float? requestsPerSecond,
        Func<int, string?, Task> progressCallbackAsync,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(sourceIndex);
        ArgumentException.ThrowIfNullOrEmpty(targetIndex);
        ArgumentNullException.ThrowIfNull(progressCallbackAsync);

        var workItem = new ReindexWorkItem
        {
            OldIndex = sourceIndex,
            NewIndex = targetIndex,
            Alias = targetIndex,
            ReindexBatchSize = batchSize,
            ReindexRequestsPerSecond = requestsPerSecond
        };

        var startResponse = await _client.ReindexAsync(d =>
        {
            d.Source(source =>
            {
                source.Indices(sourceIndex);
                if (batchSize.HasValue)
                    source.Size(batchSize.Value);
            });
            d.Dest(destination => destination.Index(targetIndex).OpType(OpType.Create).Pipeline("_none"));
            d.Conflicts(Conflicts.Abort);
            d.Refresh();
            d.Slices(SlicesCalculation.Auto);
            d.WaitForCompletion(false);

            if (requestsPerSecond.HasValue)
                d.RequestsPerSecond(requestsPerSecond.Value);
        }, cancellationToken).AnyContext();
        _logger.LogRequest(startResponse);

        if (!startResponse.IsValidResponse || startResponse.Task is null)
            throw new RepositoryException(startResponse.GetErrorMessage($"Unable to start compatibility reindex from '{sourceIndex}' to '{targetIndex}'."), startResponse.OriginalException());

        try
        {
            return await WaitForCompletionAsync(startResponse.Task, workItem, progressCallbackAsync, cancellationToken).AnyContext();
        }
        catch
        {
            await TryCancelTaskAsync(startResponse.Task, sourceIndex, targetIndex).AnyContext();
            throw;
        }
    }

    private async Task<ElasticReindexTaskResult> WaitForCompletionAsync(
        TaskId taskId,
        ReindexWorkItem workItem,
        Func<int, string?, Task> progressCallbackAsync,
        CancellationToken cancellationToken)
    {
        int statusFailures = 0;
        long lastProgress = 0;
        var noProgressTimeout = ElasticReindexer.GetNoProgressTimeout(workItem);
        var noProgressStopwatch = Stopwatch.StartNew();

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var status = await _client.Tasks.GetAsync(taskId.FullyQualifiedId, cancellationToken).AnyContext();
            if (!status.IsValidResponse)
            {
                _logger.LogErrorRequest(status, "Error getting compatibility reindex task {TaskId} status", taskId.FullyQualifiedId);
                statusFailures++;
                if (statusFailures > MaxStatusFailures)
                    throw new RepositoryException(status.GetErrorMessage($"Unable to track compatibility reindex task '{taskId.FullyQualifiedId}' after {statusFailures} attempts."), status.OriginalException());

                await _timeProvider.Delay(ElasticReindexer.GetStatusRetryDelay(statusFailures), cancellationToken).AnyContext();
                continue;
            }

            _logger.LogRequest(status);
            statusFailures = 0;

            var taskResponse = status.DeserializeRaw<TaskWithReindexResponse>(_serializer);
            if (taskResponse?.Error is not null)
                throw new RepositoryException($"Compatibility reindex task '{taskId.FullyQualifiedId}' failed: {taskResponse.Error.Type}: {taskResponse.Error.Reason}");

            var taskStatus = ReadTaskStatus(status.Task.Status);
            long completed = taskStatus.Created + taskStatus.Updated + taskStatus.Noops;
            if (completed > lastProgress)
                noProgressStopwatch.Restart();
            lastProgress = completed;

            int progress = taskStatus.Total is 0 ? 10 : 10 + (int)(80 * (double)completed / taskStatus.Total);
            await progressCallbackAsync(Math.Min(progress, 90), $"[{workItem.NewIndex}] Total: {taskStatus.Total:N0} Completed: {completed:N0} VersionConflicts: {taskStatus.VersionConflicts:N0}").AnyContext();

            if (status.Completed)
            {
                var result = taskResponse?.Response ?? throw new RepositoryException($"Compatibility reindex task '{taskId.FullyQualifiedId}' completed without a response.");
                ValidateResult(result, workItem);
                return new ElasticReindexTaskResult(result.Total, result.Created);
            }

            if (noProgressStopwatch.Elapsed > noProgressTimeout)
                throw new RepositoryException($"Compatibility reindex task '{taskId.FullyQualifiedId}' made no progress for {noProgressTimeout}.");

            var delay = taskStatus.Total < 100 ? TimeSpan.FromMilliseconds(100) : TimeSpan.FromSeconds(1);
            await _timeProvider.Delay(delay, cancellationToken).AnyContext();
        }
    }

    private static void ValidateResult(TaskReindexResult result, ReindexWorkItem workItem)
    {
        int failures = result.Failures?.Count ?? 0;
        if (failures > 0 || result.VersionConflicts > 0 || result.Created != result.Total || result.Updated > 0 || result.Deleted > 0 || result.Noops > 0)
        {
            throw new RepositoryException(
                $"Compatibility reindex from '{workItem.OldIndex}' to '{workItem.NewIndex}' was not an exact copy. " +
                $"Total: {result.Total}, Created: {result.Created}, Updated: {result.Updated}, Deleted: {result.Deleted}, Noops: {result.Noops}, " +
                $"Version conflicts: {result.VersionConflicts}, Failures: {failures}.");
        }
    }

    private static TaskStatusValues ReadTaskStatus(object? status)
    {
        if (status is JsonElement jsonElement)
        {
            return new TaskStatusValues(
                ReadInt64(jsonElement, "total"),
                ReadInt64(jsonElement, "created"),
                ReadInt64(jsonElement, "updated"),
                ReadInt64(jsonElement, "noops"),
                ReadInt64(jsonElement, "version_conflicts"));
        }

        if (status is IDictionary<string, object> values)
        {
            return new TaskStatusValues(
                ReadInt64(values, "total"),
                ReadInt64(values, "created"),
                ReadInt64(values, "updated"),
                ReadInt64(values, "noops"),
                ReadInt64(values, "version_conflicts"));
        }

        return new TaskStatusValues(0, 0, 0, 0, 0);
    }

    private static long ReadInt64(JsonElement element, string propertyName)
    {
        return element.TryGetProperty(propertyName, out var value) ? value.GetInt64() : 0;
    }

    private static long ReadInt64(IDictionary<string, object> values, string key)
    {
        return values.TryGetValue(key, out var value) ? Convert.ToInt64(value) : 0;
    }

    private async Task TryCancelTaskAsync(TaskId taskId, string sourceIndex, string targetIndex)
    {
        try
        {
            using var cleanupCancellation = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            var response = await _client.Tasks.CancelAsync(c => c.TaskId(taskId), cleanupCancellation.Token).AnyContext();
            if (response.IsValidResponse)
                _logger.LogRequest(response);
            else
                _logger.LogErrorRequest(response, "Failed to cancel compatibility reindex task {TaskId} for {SourceIndex} -> {TargetIndex}", taskId.FullyQualifiedId, sourceIndex, targetIndex);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Exception cancelling compatibility reindex task {TaskId} for {SourceIndex} -> {TargetIndex}", taskId.FullyQualifiedId, sourceIndex, targetIndex);
        }
    }

    private sealed record TaskWithReindexResponse
    {
        public TaskReindexResult? Response { get; init; }
        public TaskReindexError? Error { get; init; }
    }

    private sealed record TaskReindexError
    {
        public string? Type { get; init; }
        public string? Reason { get; init; }
    }

    private sealed record TaskReindexResult
    {
        public long Total { get; init; }
        public long Created { get; init; }
        public long Updated { get; init; }
        public long Deleted { get; init; }
        public long Noops { get; init; }
        public long VersionConflicts { get; init; }
        public IReadOnlyCollection<object>? Failures { get; init; }
    }

    private readonly record struct TaskStatusValues(long Total, long Created, long Updated, long Noops, long VersionConflicts);
}

internal readonly record struct ElasticReindexTaskResult(long Total, long Created);
