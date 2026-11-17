using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;
using Foundatio.Repositories.Queries;
using Foundatio.Utility;
using Microsoft.Extensions.Logging;
using Tasks = Elastic.Clients.Elasticsearch.Tasks;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;

namespace Foundatio.Repositories.Elasticsearch;

public abstract partial class ElasticRepositoryBase<T>
{
    /// <summary>The whole query of a delete-by-query, as the single item of its round runner.</summary>
    private sealed record QueryItem(DocumentKey Key) : IWriteItem
    {
        public ElasticDocumentVersion Version => ElasticDocumentVersion.Empty;
    }

    /// <summary>
    /// Deletes by query in passes. Each pass is one item round of the <see cref="WriteRoundRunner"/>: documents changed
    /// after the pass's snapshot are skipped as version conflicts, and because deleting is idempotent the query runs again
    /// (after backoff and a refresh) up to <c>Retry(n)</c> more times. The returned run ends as
    /// <see cref="WriteOutcome.Succeeded"/>, <see cref="WriteOutcome.Conflict"/> (conflicts left after the last pass),
    /// <see cref="WriteOutcome.Cancelled"/> or a failure. Documents deleted by earlier passes are always counted.
    /// Repository query hooks and system filters run before the query and target indexes are prepared; retries reuse them.
    /// </summary>
    private async Task<(long Deleted, long Conflicts, bool OutcomeUnknown, WriteRunResult<QueryItem> Run, IRepositoryQuery Query)> DeleteByQueryAsync(IRepositoryQuery query, ICommandOptions options)
    {
        await OnBeforeQueryAsync(query, options, typeof(T)).AnyContext();
        query = ConfigureQuery(query.As<T>()).Unwrap();
        var esQuery = await ElasticIndex.QueryBuilder.BuildQueryAsync(query, options, new SearchRequestDescriptor<T>()).AnyContext();
        string indexNames = String.Join(",", ElasticIndex.GetIndexesByQuery(query));
        var indices = Indices.Index(indexNames);
        long deleted = 0;
        long conflicts = 0;
        bool outcomeUnknown = false;
        int pass = 0;

        var run = await WriteRunner.RunAsync<QueryItem>("RemoveAll", [new QueryItem(new DocumentKey(indexNames, "*"))], options.GetRetryCount(), async (_, ct) =>
        {
            ct.ThrowIfCancellationRequested();
            pass++;
            DeleteByQueryResponse response;
            try
            {
                response = await WriteClient.DeleteByQueryAsync(new DeleteByQueryRequest(indices)
                {
                    Refresh = pass > 1 || options.GetRefreshMode(DefaultConsistency) != Refresh.False,
                    Conflicts = Conflicts.Proceed,
                    IgnoreUnavailable = true,
                    Query = esQuery,
                    RequestConfiguration = new RequestConfiguration { DisableDirectStreaming = true }
                }, ct).AnyContext();
            }
            catch (Exception ex)
            {
                outcomeUnknown = !WriteOutcomeClassifier.IsNotSent(ex);
                return [new WriteItemResult(WriteOutcome.Fatal,
                    Reason: outcomeUnknown ? "Delete by query failed, so how many documents were deleted is unknown." : ex.Message,
                    Exception: ex)];
            }

            _logger.LogRequest(response, options.GetQueryLogLevel());

            int? status = response.ApiCallDetails?.HttpStatusCode;
            if (status is >= 200 and < 300
                && (!response.IsValidResponse
                    || response.Total is not >= 0 || response.Deleted is not >= 0 || response.VersionConflicts is not >= 0
                    || response.Deleted > response.Total
                    || response.VersionConflicts > response.Total - response.Deleted
                    || response.Deleted > Int64.MaxValue - deleted))
            {
                outcomeUnknown = true;
                return [new WriteItemResult(WriteOutcome.Fatal, status,
                    Reason: "Delete by query returned an incomplete or invalid completion response, so how many documents were deleted is unknown.",
                    Exception: response.OriginalException())];
            }

            deleted += response.Deleted ?? 0;
            conflicts = response.VersionConflicts ?? 0;
            if (GetQueryFailureReason(response.TimedOut, response.Failures, response.ApiCallDetails?.ResponseBodyInBytes) is { } failure)
                return [new WriteItemResult(WriteOutcome.Fatal, response.ApiCallDetails?.HttpStatusCode, Reason: failure, Exception: response.OriginalException())];

            if (!response.IsValidResponse || response.ApiCallDetails?.HttpStatusCode is 404)
                return [ToWriteResult(response, ElasticDocumentVersion.Empty) with { Reason = response.GetErrorMessage("Error removing documents") }];

            return [conflicts > 0
                ? new WriteItemResult(WriteOutcome.Conflict, 409, Reason: $"{conflicts} documents changed after the delete-by-query snapshot")
                : new WriteItemResult(WriteOutcome.Succeeded)];
        }, async (items, ct) =>
        {
            // Refresh so the next pass's snapshot sees the versions written by concurrent writers.
            var refreshResponse = await _client.Indices.RefreshAsync(indices, cancellationToken: ct).AnyContext();
            if (!refreshResponse.IsValidResponse)
                _logger.LogErrorRequest(refreshResponse, "Failed to refresh indices before delete-by-query retry");

            return items.Select(i => ((QueryItem?)i, (WriteItemResult?)null)).ToList();
        }, DisposedCancellationToken).AnyContext();

        return (deleted, conflicts, outcomeUnknown, run, query);
    }

    /// <summary>
    /// Starts a script patch as one asynchronous update-by-query task (<see cref="PatchStrategy.UpdateByQueryBulk"/>) and
    /// returns its task id and effective query for completion effects. A failed response can leave the task's outcome unknown.
    /// Repository query hooks and system filters run before preparing the request, as they do for batch selection.
    /// </summary>
    private async Task<(string TaskId, IRepositoryQuery Query)> StartPatchAllByQueryAsync(IRepositoryQuery query, ScriptPatch scriptOperation, ICommandOptions options)
    {
        await OnBeforeQueryAsync(query, options, typeof(T)).AnyContext();
        query = ConfigureQuery(query.As<T>()).Unwrap();
        string indexNames = String.Join(",", ElasticIndex.GetIndexesByQuery(query));
        var request = new UpdateByQueryRequest(Indices.Index(indexNames))
        {
            Query = await ElasticIndex.QueryBuilder.BuildQueryAsync(query, options, new SearchRequestDescriptor<T>()).AnyContext(),
            Conflicts = Conflicts.Proceed,
            Script = new Script { Source = scriptOperation.Script, Params = scriptOperation.Params },
            Pipeline = DefaultPipeline,
            Version = HasVersion,
            Refresh = options.GetRefreshMode(DefaultConsistency) != Refresh.False,
            IgnoreUnavailable = true,
            WaitForCompletion = false
        };

        string? taskId = null;
        bool outcomeUnknown = false;
        var run = await WriteRunner.RunAsync<QueryItem>("PatchAll submission", [new QueryItem(new DocumentKey(indexNames, "*"))], options.GetRetryCount(), async (_, ct) =>
        {
            ct.ThrowIfCancellationRequested();
            UpdateByQueryResponse response;
            try
            {
                response = await WriteClient.UpdateByQueryAsync(request, ct).AnyContext();
            }
            catch (Exception ex)
            {
                outcomeUnknown = !WriteOutcomeClassifier.IsNotSent(ex);
                throw;
            }

            _logger.LogRequest(response, options.GetQueryLogLevel());
            if (!response.IsValidResponse || response.ApiCallDetails?.HttpStatusCode is 404)
            {
                var result = ToWriteResult(response, ElasticDocumentVersion.Empty);
                int? status = response.ApiCallDetails?.HttpStatusCode;
                outcomeUnknown = status is null or (>= 200 and < 300) or 408 or >= 500 && result.Outcome is WriteOutcome.Fatal;
                return [result with { Outcome = result.Outcome is WriteOutcome.Missing ? WriteOutcome.Fatal : result.Outcome, Reason = response.GetErrorMessage("Error occurred while patching by query") }];
            }

            taskId = response.Task?.ToString();
            if (String.IsNullOrEmpty(taskId))
            {
                outcomeUnknown = true;
                return [new WriteItemResult(WriteOutcome.Fatal, Reason: "Update by query did not return a task identifier.")];
            }

            return [new WriteItemResult(WriteOutcome.Succeeded)];
        }, null, DisposedCancellationToken).AnyContext();

        if (outcomeUnknown)
            await NotifyPatchedByQuerySafelyAsync(query, options).AnyContext();

        WriteExceptionMapper.ThrowIfFailed(run, "starting query patch", MissingDocumentPolicy.NotFound);
        return (taskId ?? throw new DocumentException("Update by query did not return a task identifier."), query);
    }

    /// <summary>
    /// Waits for an update-by-query task started by <see cref="StartPatchAllByQueryAsync"/>. Documents changed after the
    /// task's snapshot are skipped and counted as conflicts, never retried, because the script may not be idempotent.
    /// </summary>
    /// <exception cref="OperationCanceledException">The repository was disposed while waiting. The task keeps running
    /// in Elasticsearch, so how many documents it patched is unknown.</exception>
    /// <exception cref="DocumentException">The task failed or its status could not be read (including a task that
    /// disappeared): the outcome is unknown, so it is never reported as zero documents patched.</exception>
    private async Task<(long Affected, long Conflicts)> WaitForPatchAllByQueryAsync(string taskId, ICommandOptions options)
    {
        var failedReads = new ConsecutiveFailureCounter(20);
        for (int attempt = 1; ; attempt++)
        {
            DisposedCancellationToken.ThrowIfCancellationRequested();
            var request = new Tasks.GetTasksRequest(taskId)
            {
                WaitForCompletion = false,
                RequestConfiguration = new RequestConfiguration { ThrowExceptions = false }
            };
            var taskStatus = await _client.Tasks.GetAsync(request, DisposedCancellationToken).AnyContext();
            _logger.LogRequest(taskStatus, options.GetQueryLogLevel());

            if (!taskStatus.IsValidResponse)
            {
                if (taskStatus.ApiCallDetails.HttpStatusCode is 404)
                    throw new DocumentException($"Update by query task {taskId} was not found, so how many documents it patched is unknown.", taskStatus.OriginalException());

                _logger.LogError("Error getting task status for {TaskId}: {Error}", taskId, taskStatus.ElasticsearchServerError);
                if (failedReads.RecordFailure())
                    throw new DocumentException($"Failed to get task status for {taskId} after {failedReads.Count} consecutive attempts", taskStatus.OriginalException());
            }
            else
            {
                failedReads.RecordSuccess();
                if (taskStatus.Completed)
                {
                    if (taskStatus.Error is not null)
                        throw new DocumentException($"Script operation task ({taskId}) failed: {taskStatus.Error.Type} - {taskStatus.Error.Reason}", taskStatus.OriginalException());

                    var status = ReadTaskStatus(taskStatus.Response)
                        ?? throw new DocumentException($"Update by query task {taskId} completed without readable result counters, so how many documents it patched is unknown.", taskStatus.OriginalException());
                    long affected = checked(status.Created + status.Updated + status.Deleted);
                    string? cancellationReason = TaskResponseFailures.GetCancellationReason(taskStatus.Response)
                        ?? TaskResponseFailures.GetCancellationReason(taskStatus.Task?.Status);
                    if (cancellationReason is not null)
                        throw new DocumentException($"Script operation task ({taskId}) was canceled after patching {affected} documents: {cancellationReason}", taskStatus.OriginalException());

                    if (TaskResponseFailures.IsTimedOut(taskStatus.Response))
                        throw new DocumentException($"Script operation task ({taskId}) timed out after patching {affected} documents.", taskStatus.OriginalException());

                    var failures = TaskResponseFailures.Read(taskStatus.Response);
                    if (failures.Count > 0)
                        throw new DocumentException($"Script operation task ({taskId}) completed with {failures.Count} failures after patching {affected} documents: {TaskResponseFailures.Summarize(failures)}", taskStatus.OriginalException());

                    _logger.LogInformation("Script operation task ({TaskId}) completed: Created: {Created} Updated: {Updated} Deleted: {Deleted} Conflicts: {Conflicts} Total: {Total}", taskId, status.Created, status.Updated, status.Deleted, status.VersionConflicts, status.Total);
                    return (affected, status.VersionConflicts);
                }

                var progress = ReadTaskStatus(taskStatus.Task?.Status);
                _logger.LogDebug("Checking script operation task ({TaskId}) status: Updated: {Updated} Conflicts: {Conflicts} Total: {Total}", taskId, progress?.Updated, progress?.VersionConflicts, progress?.Total);
            }

            await ElasticIndex.Configuration.TimeProvider.SafeDelay(TimeSpan.FromSeconds(attempt <= 5 ? 1 : 5), DisposedCancellationToken).AnyContext();
        }
    }

    private readonly record struct QueryTaskStatus(long Total, long Created, long Updated, long Deleted, long VersionConflicts);

    private static string? GetQueryFailureReason(bool? timedOut, IReadOnlyCollection<BulkIndexByScrollFailure>? failures, byte[]? responseBody)
    {
        if (TaskResponseFailures.GetCancellationReason(responseBody) is { } cancellationReason)
            return $"The query was canceled before all matching documents were processed: {cancellationReason}";

        if (timedOut is true)
            return "The query timed out before all matching documents were processed.";

        if (failures is not { Count: > 0 })
            return null;

        var descriptions = failures.Select(f => $"{f.Index}/{f.Id}: {f.Cause.Type} - {f.Cause.Reason}").ToList();
        return $"The query completed with {failures.Count} failures: {TaskResponseFailures.Summarize(descriptions)}";
    }

    /// <summary>
    /// Reads a complete set of nonnegative task counters. Native task status and completed response objects may be
    /// <see cref="JsonElement"/> values or dictionaries depending on the serializer configuration.
    /// </summary>
    private static QueryTaskStatus? ReadTaskStatus(object? status)
    {
        long? total = Read(status, "total");
        long? created = Read(status, "created");
        long? updated = Read(status, "updated");
        long? deleted = Read(status, "deleted");
        long? conflicts = Read(status, "version_conflicts");
        if (total is null || created is null || updated is null || deleted is null || conflicts is null)
            return null;

        return new QueryTaskStatus(total.Value, created.Value, updated.Value, deleted.Value, conflicts.Value);

        static long? Read(object? source, string name)
        {
            object? value = source switch
            {
                JsonElement { ValueKind: JsonValueKind.Object } element when element.TryGetProperty(name, out var counter) => counter,
                IDictionary<string, object> dict when dict.TryGetValue(name, out var counter) => counter,
                _ => null
            };

            if (value is JsonElement number)
                return number.ValueKind is JsonValueKind.Number && number.TryGetInt64(out long count) && count >= 0 ? count : null;

            if (value is not (byte or sbyte or short or ushort or int or uint or long or ulong or float or double or decimal))
                return null;

            decimal converted = Convert.ToDecimal(value);
            return converted >= 0 && converted <= Int64.MaxValue && Decimal.Truncate(converted) == converted ? (long)converted : null;
        }
    }
}
