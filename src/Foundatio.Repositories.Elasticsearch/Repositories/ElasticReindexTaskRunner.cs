using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Jobs;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Foundatio.Repositories.Elasticsearch;

internal sealed class ElasticReindexTaskRunner
{
    private const int MaxStatusFailures = 10;
    private readonly ElasticsearchClient _client;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger _logger;

    public ElasticReindexTaskRunner(ElasticsearchClient client, TimeProvider timeProvider, ILogger? logger = null)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
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
        ValidateOptions(batchSize, requestsPerSecond);

        var workItem = new ReindexWorkItem
        {
            OldIndex = sourceIndex,
            NewIndex = targetIndex,
            Alias = targetIndex,
            ReindexBatchSize = batchSize,
            ReindexRequestsPerSecond = requestsPerSecond
        };

        ReindexResponse startResponse;
        try
        {
            startResponse = await _client.ReindexAsync(d =>
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
        }
        catch (Exception ex)
        {
            throw CreateUncertainStartException(sourceIndex, targetIndex, ex);
        }

        _logger.LogRequest(startResponse);

        if (!startResponse.IsValidResponse || startResponse.Task is null)
        {
            var startException = startResponse.OriginalException()
                ?? new RepositoryException(startResponse.GetErrorMessage($"Elasticsearch did not return a task ID for compatibility reindex from '{sourceIndex}' to '{targetIndex}'."));
            throw CreateUncertainStartException(sourceIndex, targetIndex, startException);
        }

        TaskReindexResult result;
        try
        {
            result = await WaitForCompletionAsync(startResponse.Task, workItem, progressCallbackAsync, cancellationToken).AnyContext();
        }
        catch (ElasticReindexTaskTerminalException)
        {
            throw;
        }
        catch (Exception reindexException)
        {
            await ElasticReindexTaskCancellation.CancelAndConfirmAsync(
                _client,
                _logger,
                startResponse.Task,
                sourceIndex,
                targetIndex,
                reindexException).AnyContext();
            throw;
        }

        ValidateResult(result, workItem);
        return new ElasticReindexTaskResult(result.Total, result.Created);
    }

    internal static void ValidateOptions(int? batchSize, float? requestsPerSecond)
    {
        if (batchSize is <= 0)
            throw new ArgumentOutOfRangeException(nameof(batchSize), batchSize, "Must be greater than zero when specified.");

        if (requestsPerSecond is float value && (value <= 0 || float.IsNaN(value) || float.IsInfinity(value)))
            throw new ArgumentOutOfRangeException(nameof(requestsPerSecond), requestsPerSecond, "Must be a positive, finite number when specified.");
    }

    private static ElasticReindexTaskUncertainException CreateUncertainStartException(string sourceIndex, string targetIndex, Exception innerException)
    {
        return new ElasticReindexTaskUncertainException(
            $"The compatibility reindex start outcome for '{sourceIndex}' -> '{targetIndex}' is unknown because no task ID was confirmed. Keep the source write blocked and retain the destination until matching Elasticsearch tasks have been inspected.",
            innerException);
    }

    private async Task<TaskReindexResult> WaitForCompletionAsync(
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
            if (status.ApiCallDetails.HttpStatusCode is 404 || !status.IsValidResponse)
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

            if (status.Error is not null)
            {
                throw new ElasticReindexTaskTerminalException(
                    $"Compatibility reindex task '{taskId.FullyQualifiedId}' failed: {status.Error.Type}: {status.Error.Reason}");
            }

            var taskStatus = ReadTaskStatus(status.Task.Status);
            long completed = taskStatus.Created + taskStatus.Updated + taskStatus.Deleted + taskStatus.Noops + taskStatus.VersionConflicts;
            if (completed > lastProgress)
                noProgressStopwatch.Restart();
            lastProgress = completed;

            int progress = taskStatus.Total is 0 ? 10 : 10 + (int)(80 * (double)completed / taskStatus.Total);
            await progressCallbackAsync(Math.Min(progress, 90), $"[{workItem.NewIndex}] Total: {taskStatus.Total:N0} Completed: {completed:N0} VersionConflicts: {taskStatus.VersionConflicts:N0}").AnyContext();

            if (status.Completed)
            {
                return ElasticTaskResponseParser.Deserialize<TaskReindexResult>(status.Response)
                    ?? throw new ElasticReindexTaskTerminalException($"Compatibility reindex task '{taskId.FullyQualifiedId}' completed without a response.");
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
                ReadInt64(jsonElement, "deleted"),
                ReadInt64(jsonElement, "noops"),
                ReadInt64(jsonElement, "version_conflicts"));
        }

        if (status is IDictionary<string, object> values)
        {
            return new TaskStatusValues(
                ReadInt64(values, "total"),
                ReadInt64(values, "created"),
                ReadInt64(values, "updated"),
                ReadInt64(values, "deleted"),
                ReadInt64(values, "noops"),
                ReadInt64(values, "version_conflicts"));
        }

        return new TaskStatusValues(0, 0, 0, 0, 0, 0);
    }

    private static long ReadInt64(JsonElement element, string propertyName)
    {
        return element.TryGetProperty(propertyName, out var value) ? value.GetInt64() : 0;
    }

    private static long ReadInt64(IDictionary<string, object> values, string key)
    {
        return values.TryGetValue(key, out var value) ? Convert.ToInt64(value) : 0;
    }

    private sealed record TaskReindexResult
    {
        public long Total { get; init; }
        public long Created { get; init; }
        public long Updated { get; init; }
        public long Deleted { get; init; }
        public long Noops { get; init; }
        [JsonPropertyName("version_conflicts")]
        public long VersionConflicts { get; init; }
        public IReadOnlyCollection<object>? Failures { get; init; }
    }

    private readonly record struct TaskStatusValues(long Total, long Created, long Updated, long Deleted, long Noops, long VersionConflicts);

    private sealed class ElasticReindexTaskTerminalException : RepositoryException
    {
        public ElasticReindexTaskTerminalException(string message) : base(message) { }
    }
}

internal readonly record struct ElasticReindexTaskResult(long Total, long Created);

internal sealed class ElasticReindexTaskUncertainException : RepositoryException
{
    public ElasticReindexTaskUncertainException(string message, Exception innerException) : base(message, innerException) { }
}
