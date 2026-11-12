using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
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
    /// </summary>
    private async Task<(long Deleted, long Conflicts, WriteRunResult<QueryItem> Run)> DeleteByQueryAsync(IRepositoryQuery query, ICommandOptions options)
    {
        var esQuery = await ElasticIndex.QueryBuilder.BuildQueryAsync(query, options, new SearchRequestDescriptor<T>()).AnyContext();
        string indexNames = String.Join(",", ElasticIndex.GetIndexesByQuery(query));
        var indices = Indices.Index(indexNames);
        long deleted = 0;
        long conflicts = 0;
        int pass = 0;

        var run = await WriteRunner.RunAsync<QueryItem>("RemoveAll", [new QueryItem(new DocumentKey(indexNames, "*"))], options.GetRetryCount(), async (_, ct) =>
        {
            pass++;
            var response = await _client.DeleteByQueryAsync(new DeleteByQueryRequest(indices)
            {
                Refresh = pass > 1 || options.GetRefreshMode(DefaultConsistency) != Refresh.False,
                Conflicts = Conflicts.Proceed,
                IgnoreUnavailable = true,
                Query = esQuery
            }, ct).AnyContext();
            _logger.LogRequest(response, options.GetQueryLogLevel());

            if (!response.IsValidResponse)
                return [ToWriteResult(response, ElasticDocumentVersion.Empty) with { Reason = response.GetErrorMessage("Error removing documents") }];

            deleted += response.Deleted ?? 0;
            conflicts = response.VersionConflicts ?? 0;
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

        return (deleted, conflicts, run);
    }

    /// <summary>
    /// Runs a script patch as one asynchronous update-by-query task (<see cref="PatchStrategy.UpdateByQueryBulk"/>) and
    /// waits for it. Documents changed after the task's snapshot are skipped and counted as conflicts, never retried,
    /// because the script may not be idempotent.
    /// </summary>
    /// <exception cref="OperationCanceledException">The repository was disposed while waiting. The task keeps running
    /// in Elasticsearch, so how many documents it patched is unknown.</exception>
    /// <exception cref="DocumentException">The task failed or its status could not be read (including a task that
    /// disappeared): the outcome is unknown, so it is never reported as zero documents patched.</exception>
    private async Task<(long Affected, long Conflicts)> PatchAllWithUpdateByQueryAsync(IRepositoryQuery query, ScriptPatch scriptOperation, ICommandOptions options)
    {
        var request = new UpdateByQueryRequest(Indices.Index(String.Join(",", ElasticIndex.GetIndexesByQuery(query))))
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

        var response = await _client.UpdateByQueryAsync(request, DisposedCancellationToken).AnyContext();
        _logger.LogRequest(response, options.GetQueryLogLevel());
        if (!response.IsValidResponse)
            throw new DocumentException(response.GetErrorMessage("Error occurred while patching by query"), response.OriginalException());

        if (response.Task is null)
            throw new DocumentException("Update by query did not return a task identifier.", response.OriginalException());

        string taskId = response.Task.ToString();
        int failedReads = 0;
        for (int attempt = 1; ; attempt++)
        {
            DisposedCancellationToken.ThrowIfCancellationRequested();
            var taskStatus = await _client.Tasks.GetAsync(new Tasks.GetTasksRequest(taskId) { WaitForCompletion = false }, DisposedCancellationToken).AnyContext();
            _logger.LogRequest(taskStatus, options.GetQueryLogLevel());

            if (!taskStatus.IsValidResponse)
            {
                if (taskStatus.ApiCallDetails.HttpStatusCode is 404)
                    throw new DocumentException($"Update by query task {taskId} was not found, so how many documents it patched is unknown.", taskStatus.OriginalException());

                _logger.LogError("Error getting task status for {TaskId}: {Error}", taskId, taskStatus.ElasticsearchServerError);
                if (++failedReads >= 20)
                    throw new DocumentException($"Failed to get task status for {taskId} after {failedReads} attempts", taskStatus.OriginalException());
            }
            else
            {
                var status = ReadTaskStatus(taskStatus.Task.Status);
                if (taskStatus.Completed)
                {
                    if (taskStatus.Error is not null)
                        throw new DocumentException($"Script operation task ({taskId}) failed: {taskStatus.Error.Type} - {taskStatus.Error.Reason}", taskStatus.OriginalException());

                    _logger.LogInformation("Script operation task ({TaskId}) completed: Created: {Created} Updated: {Updated} Deleted: {Deleted} Conflicts: {Conflicts} Total: {Total}", taskId, status.Created, status.Updated, status.Deleted, status.VersionConflicts, status.Total);
                    return (status.Created + status.Updated + status.Deleted, status.VersionConflicts);
                }

                _logger.LogDebug("Checking script operation task ({TaskId}) status: Updated: {Updated} Conflicts: {Conflicts} Total: {Total}", taskId, status.Updated, status.VersionConflicts, status.Total);
            }

            await ElasticIndex.Configuration.TimeProvider.SafeDelay(TimeSpan.FromSeconds(attempt <= 5 ? 1 : 5), DisposedCancellationToken).AnyContext();
        }
    }

    private readonly record struct QueryTaskStatus(long Total, long Created, long Updated, long Deleted, long VersionConflicts);

    /// <summary>
    /// Reads task status counters. The status is <c>object?</c> and may be a <see cref="JsonElement"/> or a dictionary
    /// depending on the serializer configuration.
    /// </summary>
    private static QueryTaskStatus ReadTaskStatus(object? status)
    {
        return status switch
        {
            JsonElement element => new QueryTaskStatus(Read(element, "total"), Read(element, "created"), Read(element, "updated"), Read(element, "deleted"), Read(element, "version_conflicts")),
            IDictionary<string, object> dict => new QueryTaskStatus(ReadValue(dict, "total"), ReadValue(dict, "created"), ReadValue(dict, "updated"), ReadValue(dict, "deleted"), ReadValue(dict, "version_conflicts")),
            _ => default
        };

        static long Read(JsonElement element, string name) => element.TryGetProperty(name, out var value) ? value.GetInt64() : 0;
        static long ReadValue(IDictionary<string, object> dict, string name) => dict.TryGetValue(name, out var value) ? Convert.ToInt64(value) : 0;
    }
}
