using System;
using System.Diagnostics;
using System.IO.Hashing;
using System.Text;
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
    internal const string OpaqueIdHeader = "X-Opaque-Id";
    private const int MaxStatusFailures = 10;
    private readonly ElasticsearchClient _client;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger _logger;

    public ElasticReindexTaskRunner(ElasticsearchClient client, TimeProvider timeProvider, ILogger? logger = null)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(timeProvider);

        _client = client;
        _timeProvider = timeProvider;
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
        string opaqueId = GetOpaqueId(sourceIndex, targetIndex);

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
                d.RequestConfiguration(request => request.OpaqueId(opaqueId));

                if (requestsPerSecond.HasValue)
                    d.RequestsPerSecond(requestsPerSecond.Value);
            }, cancellationToken).AnyContext();
        }
        catch (Exception ex)
        {
            throw CreateUncertainStartException(sourceIndex, targetIndex, ex);
        }

        if (!startResponse.IsValidResponse || startResponse.Task is null)
        {
            _logger.LogErrorRequest(startResponse, "Unable to start compatibility reindex from {SourceIndex} to {TargetIndex}", sourceIndex, targetIndex);
            var startException = startResponse.OriginalException()
                ?? new RepositoryException(startResponse.GetErrorMessage($"Elasticsearch did not return a task ID for compatibility reindex from '{sourceIndex}' to '{targetIndex}'."));
            throw CreateUncertainStartException(sourceIndex, targetIndex, startException);
        }

        _logger.LogRequest(startResponse);

        ElasticReindexTaskResponse result;
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

    private async Task<ElasticReindexTaskResponse> WaitForCompletionAsync(
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

            var taskStatus = ElasticReindexTaskResponseReader.ReadStatus(status.Task.Status)
                ?? throw new ElasticReindexTaskTerminalException(
                    $"Compatibility reindex task '{taskId.FullyQualifiedId}' returned an unrecognized status payload.");
            long completed = taskStatus.Created + taskStatus.Updated + taskStatus.Deleted + taskStatus.Noops + taskStatus.VersionConflicts;
            if (completed > lastProgress)
                noProgressStopwatch.Restart();
            lastProgress = completed;

            int progress = taskStatus.Total is 0 ? 10 : 10 + (int)(80 * (double)completed / taskStatus.Total);
            await progressCallbackAsync(Math.Min(progress, 90), $"[{workItem.NewIndex}] Total: {taskStatus.Total:N0} Completed: {completed:N0} VersionConflicts: {taskStatus.VersionConflicts:N0}").AnyContext();

            if (status.Completed)
            {
                return ElasticReindexTaskResponseReader.ReadCompleted(status.Response)
                    ?? throw new ElasticReindexTaskTerminalException(
                        $"Compatibility reindex task '{taskId.FullyQualifiedId}' completed without a recognized response.");
            }

            if (noProgressStopwatch.Elapsed > noProgressTimeout)
                throw new RepositoryException($"Compatibility reindex task '{taskId.FullyQualifiedId}' made no progress for {noProgressTimeout}.");

            var delay = taskStatus.Total < 100 ? TimeSpan.FromMilliseconds(100) : TimeSpan.FromSeconds(1);
            await _timeProvider.Delay(delay, cancellationToken).AnyContext();
        }
    }

    private static void ValidateResult(ElasticReindexTaskResponse result, ReindexWorkItem workItem)
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

    internal static string GetOpaqueId(string sourceIndex, string targetIndex)
    {
        ArgumentException.ThrowIfNullOrEmpty(sourceIndex);
        ArgumentException.ThrowIfNullOrEmpty(targetIndex);

        var hasher = new XxHash64();
        hasher.Append(Encoding.UTF8.GetBytes(sourceIndex));
        hasher.Append([0]);
        hasher.Append(Encoding.UTF8.GetBytes(targetIndex));
        return $"foundatio-compat-{hasher.GetCurrentHashAsUInt64():x16}";
    }

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
