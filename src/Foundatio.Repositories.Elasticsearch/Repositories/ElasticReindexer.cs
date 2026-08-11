using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Core.Search;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Clients.Elasticsearch.Mapping;
using Elastic.Clients.Elasticsearch.QueryDsl;
using Elastic.Transport.Products.Elasticsearch;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Jobs;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Utility;
using Foundatio.Resilience;
using Foundatio.Serializer;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Foundatio.Repositories.Elasticsearch;

public class ElasticReindexer
{
    private readonly ElasticsearchClient _client;
    private readonly ITextSerializer _serializer;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger _logger;
    private readonly IResiliencePolicyProvider _resiliencePolicyProvider;
    private readonly IResiliencePolicy _resiliencePolicy;
    private const string ID_FIELD = "id";
    private const int MAX_STATUS_FAILS = 10;

    /// <summary>
    /// Returns the distributed lock resource name for serializing reindex operations on the given alias.
    /// </summary>
    public static string GetLockName(string alias)
    {
        ArgumentException.ThrowIfNullOrEmpty(alias);

        return String.Concat("reindex:", alias);
    }

    public ElasticReindexer(ElasticsearchClient client, ITextSerializer serializer, ILogger? logger = null) : this(client, serializer, TimeProvider.System, logger)
    {
    }

    public ElasticReindexer(ElasticsearchClient client, ITextSerializer serializer, TimeProvider timeProvider, ILogger? logger = null) : this(client, serializer, timeProvider ?? TimeProvider.System, new ResiliencePolicyProvider(), logger ?? NullLogger.Instance)
    {
    }

    public ElasticReindexer(ElasticsearchClient client, ITextSerializer serializer, TimeProvider timeProvider, IResiliencePolicyProvider resiliencePolicyProvider, ILogger? logger = null)
    {
        _client = client;
        _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        _timeProvider = timeProvider ?? TimeProvider.System;
        _resiliencePolicyProvider = resiliencePolicyProvider ?? new ResiliencePolicyProvider();
        _logger = logger ?? NullLogger.Instance;

        _resiliencePolicy = _resiliencePolicyProvider.GetPolicy<ElasticReindexer>(fallback => fallback.WithMaxAttempts(5).WithDelay(TimeSpan.FromSeconds(10)), _logger, _timeProvider);
    }

    public async Task ReindexAsync(ReindexWorkItem workItem, Func<int, string?, Task>? progressCallbackAsync = null)
    {
        ArgumentNullException.ThrowIfNull(workItem);

        if (String.IsNullOrEmpty(workItem.OldIndex))
            throw new ArgumentNullException(nameof(workItem.OldIndex));

        if (String.IsNullOrEmpty(workItem.NewIndex))
            throw new ArgumentNullException(nameof(workItem.NewIndex));

        if (workItem.ReindexBatchSize is <= 0)
            throw new ArgumentOutOfRangeException(nameof(workItem.ReindexBatchSize), workItem.ReindexBatchSize, "Must be greater than zero when specified.");

        // Checked explicitly (rather than a `float.NaN` constant pattern) so the intent - and the fact
        // that infinities are rejected alongside NaN - is obvious without knowing pattern-matching's
        // NaN semantics. `<= 0` alone wouldn't catch +Infinity, since +Infinity > 0.
        if (workItem.ReindexRequestsPerSecond is float requestsPerSecond && (requestsPerSecond <= 0 || float.IsNaN(requestsPerSecond) || float.IsInfinity(requestsPerSecond)))
            throw new ArgumentOutOfRangeException(nameof(workItem.ReindexRequestsPerSecond), workItem.ReindexRequestsPerSecond, "Must be a positive, finite number when specified.");

        if (progressCallbackAsync == null)
        {
            progressCallbackAsync = (progress, message) =>
            {
                _logger.LogInformation("Reindex Progress {Progress:F1}%: {Message}", progress, message);
                return Task.CompletedTask;
            };
        }

        using var _ = _logger.BeginScope(new Dictionary<string, object>
        {
            [nameof(workItem.OldIndex)] = workItem.OldIndex,
            [nameof(workItem.NewIndex)] = workItem.NewIndex,
            [nameof(workItem.Alias)] = workItem.Alias
        });

        _logger.LogInformation("Received reindex work item for {OldIndex} -> {NewIndex}", workItem.OldIndex, workItem.NewIndex);
        string concreteOldIndex = workItem.OldIndex;
        Dictionary<string, AliasDefinition>? aliases = null;
        if (workItem.OldIndex != workItem.NewIndex)
        {
            concreteOldIndex = await ResolveConcreteIndexAsync(workItem.OldIndex).AnyContext();
            if (String.Equals(concreteOldIndex, workItem.NewIndex, StringComparison.Ordinal))
            {
                _logger.LogInformation("Skipping stale reindex work item because {OldIndex} already resolves to {NewIndex}", workItem.OldIndex, workItem.NewIndex);
                await progressCallbackAsync(100, "Reindex already complete").AnyContext();
                return;
            }

            aliases = await GetIndexAliasesAsync(concreteOldIndex).AnyContext();
            if (aliases.TryGetValue(workItem.OldIndex, out var sourceAlias) && IsFilteredOrRouted(sourceAlias))
                throw new InvalidOperationException($"Cannot reindex filtered or routed alias '{workItem.OldIndex}'. Reindex its concrete index '{concreteOldIndex}' so catch-up reads use the same complete source.");
        }

        // Keep the caller's source name for _reindex so filtered/routed aliases retain their semantics.
        // Use concreteOldIndex only for alias movement, counts, sampling, and deletion below.
        var sourceWorkItem = workItem;
        var startTime = _timeProvider.GetUtcNow().UtcDateTime.AddSeconds(-1);
        await progressCallbackAsync(0, "Starting reindex...").AnyContext();
        var firstPassResult = await InternalReindexAsync(sourceWorkItem, progressCallbackAsync, 0, 90, workItem.StartUtc).AnyContext();

        if (!firstPassResult.Succeeded)
            return;

        await progressCallbackAsync(91, $"Total: {firstPassResult.Total:N0} Completed: {firstPassResult.Completed:N0}").AnyContext();

        SampleIdResult? sampleResult = null;
        if (String.IsNullOrEmpty(workItem.TimestampField))
        {
            sampleResult = await GetSampleDocumentIdAsync(concreteOldIndex).AnyContext();
            if (workItem.OldIndex != workItem.NewIndex
                && sampleResult.Status is SampleIdStatus.Found
                && !ObjectId.TryParse(sampleResult.Id!, out var unused))
            {
                throw new RepositoryException(
                    $"Reindex '{workItem.OldIndex}' -> '{workItem.NewIndex}' cannot safely catch up writes because no TimestampField is configured and document IDs are not ObjectIds.");
            }

            if (workItem.OldIndex != workItem.NewIndex && sampleResult.Status is SampleIdStatus.Failed)
                throw new RepositoryException($"Unable to establish a safe catch-up strategy for reindex '{workItem.OldIndex}' -> '{workItem.NewIndex}'.", sampleResult.Exception!);
        }

        if (workItem.OldIndex != workItem.NewIndex)
        {
            aliases ??= await GetIndexAliasesAsync(concreteOldIndex).AnyContext();
            // Older compatibility implementations could leave an alias matching the physical source name.
            // It is a migration artifact, not a stable application alias, and must not leak into the new schema.
            aliases.Remove(concreteOldIndex);
            if (!String.IsNullOrEmpty(workItem.Alias) && !aliases.ContainsKey(workItem.Alias))
                aliases.Add(workItem.Alias, new AliasDefinition());

            if (!await MoveAliasesAsync(concreteOldIndex, workItem.NewIndex, aliases, progressCallbackAsync).AnyContext())
                return;

            // Writes now route to the destination. Catch up from the resolved physical source so an alias move
            // cannot redirect the second pass into the destination itself.
            sourceWorkItem = workItem with { OldIndex = concreteOldIndex };
        }

        var refreshResponse = await _client.Indices.RefreshAsync(Indices.All).AnyContext();
        _logger.LogRequest(refreshResponse);
        if (!refreshResponse.IsValidResponse)
            _logger.LogWarning("Failed to refresh indices before second reindex pass for {OldIndex} -> {NewIndex}: {Error}", workItem.OldIndex, workItem.NewIndex, refreshResponse.ElasticsearchServerError);

        ReindexResult? secondPassResult = null;
        if (!String.IsNullOrEmpty(workItem.TimestampField))
        {
            secondPassResult = await InternalReindexAsync(sourceWorkItem, progressCallbackAsync, 93, 97, startTime).AnyContext();
            if (!secondPassResult.Succeeded)
                return;

            await progressCallbackAsync(98, $"Total: {secondPassResult.Total:N0} Completed: {secondPassResult.Completed:N0}").AnyContext();
        }
        else
        {
            async Task RunObjectIdSecondPassAsync()
            {
                secondPassResult = await InternalReindexAsync(sourceWorkItem, progressCallbackAsync, 93, 97, startTime).AnyContext();
                if (!secondPassResult.Succeeded)
                    return;

                await progressCallbackAsync(98, $"Total: {secondPassResult.Total:N0} Completed: {secondPassResult.Completed:N0}").AnyContext();
            }

            switch (sampleResult!.Status)
            {
                case SampleIdStatus.Empty:
                    _logger.LogInformation("Reindex {OldIndex} -> {NewIndex}: Source index is empty, skipping second pass.", workItem.OldIndex, workItem.NewIndex);
                    break;

                case SampleIdStatus.Found when ObjectId.TryParse(sampleResult.Id!, out var unused):
                    _logger.LogInformation("Reindex {OldIndex} -> {NewIndex}: Using ObjectId-based second pass (no TimestampField).", workItem.OldIndex, workItem.NewIndex);
                    await RunObjectIdSecondPassAsync().AnyContext();
                    break;

                case SampleIdStatus.Found:
                    _logger.LogWarning(
                        "Reindex {OldIndex} -> {NewIndex}: No TimestampField and IDs are not ObjectIds (sample: {SampleId}). Cannot perform second-pass catch-up.",
                        workItem.OldIndex, workItem.NewIndex, sampleResult.Id);
                    break;

                case SampleIdStatus.Failed:
                    _logger.LogWarning(sampleResult.Exception,
                        "Reindex {OldIndex} -> {NewIndex}: Failed to sample document ID ({Error}). Attempting ObjectId-based second pass anyway.",
                        workItem.OldIndex, workItem.NewIndex, sampleResult.Error);
                    await RunObjectIdSecondPassAsync().AnyContext();
                    break;
            }

            if (secondPassResult is { Succeeded: false })
                return;
        }

        long totalFailures = firstPassResult.Failures;
        if (secondPassResult != null)
            totalFailures += secondPassResult.Failures;

        bool hasFailures = totalFailures > 0;
        if (!hasFailures && workItem.DeleteOld && workItem.OldIndex != workItem.NewIndex)
        {
            refreshResponse = await _client.Indices.RefreshAsync(Indices.All).AnyContext();
            _logger.LogRequest(refreshResponse);
            if (!refreshResponse.IsValidResponse)
                _logger.LogWarning("Failed to refresh indices before doc count comparison for {OldIndex} -> {NewIndex}: {Error}", workItem.OldIndex, workItem.NewIndex, refreshResponse.ElasticsearchServerError);

            var newDocCountResponse = await _client.CountAsync<object>(d => d.Indices(workItem.NewIndex)).AnyContext();
            _logger.LogRequest(newDocCountResponse);
            if (!newDocCountResponse.IsValidResponse)
                _logger.LogWarning("Failed to get new index doc count for {NewIndex}: {Error}", workItem.NewIndex, newDocCountResponse.ElasticsearchServerError);

            var oldDocCountResponse = await _client.CountAsync<object>(d => d.Indices(concreteOldIndex)).AnyContext();
            _logger.LogRequest(oldDocCountResponse);
            if (!oldDocCountResponse.IsValidResponse)
                _logger.LogWarning("Failed to get old index doc count for {OldIndex}: {Error}", concreteOldIndex, oldDocCountResponse.ElasticsearchServerError);

            await progressCallbackAsync(98, $"Old Docs: {oldDocCountResponse.Count} New Docs: {newDocCountResponse.Count}").AnyContext();
            if (newDocCountResponse.IsValidResponse && oldDocCountResponse.IsValidResponse && newDocCountResponse.Count >= oldDocCountResponse.Count)
            {
                var deleteIndexResponse = await _client.Indices.DeleteAsync(Indices.Index(concreteOldIndex)).AnyContext();
                _logger.LogRequest(deleteIndexResponse);
                if (!deleteIndexResponse.IsValidResponse)
                    _logger.LogWarning("Failed to delete old index {OldIndex}: {Error}", concreteOldIndex, deleteIndexResponse.ElasticsearchServerError);

                if (deleteIndexResponse.IsValidResponse)
                    await progressCallbackAsync(99, $"Deleted index: {concreteOldIndex}").AnyContext();
                else
                    await progressCallbackAsync(99, $"Failed to delete old index {concreteOldIndex}: {deleteIndexResponse.ElasticsearchServerError}").AnyContext();
            }
        }

        await progressCallbackAsync(100, "Reindex complete").AnyContext();
    }

    private static bool IsFilteredOrRouted(AliasDefinition alias)
    {
        return alias.Filter is not null
            || !String.IsNullOrEmpty(alias.IndexRouting)
            || !String.IsNullOrEmpty(alias.Routing)
            || !String.IsNullOrEmpty(alias.SearchRouting);
    }

    private async Task<bool> MoveAliasesAsync(
        string oldIndex,
        string newIndex,
        IReadOnlyDictionary<string, AliasDefinition> aliases,
        Func<int, string?, Task> progressCallbackAsync)
    {
        if (aliases.Count is 0)
            return true;

        var aliasActions = new List<IndexUpdateAliasesAction>(aliases.Count * 2);
        foreach (var alias in aliases)
        {
            aliasActions.Add(new IndexUpdateAliasesAction { Remove = new RemoveAction { Alias = alias.Key, Index = oldIndex } });
            var addAction = new AddAction
            {
                Alias = alias.Key,
                Index = newIndex,
                Filter = alias.Value.Filter,
                IsHidden = alias.Value.IsHidden,
                IsWriteIndex = alias.Value.IsWriteIndex
            };
            if (!String.IsNullOrEmpty(alias.Value.IndexRouting))
                addAction.IndexRouting = alias.Value.IndexRouting;
            if (!String.IsNullOrEmpty(alias.Value.Routing))
                addAction.Routing = alias.Value.Routing;
            if (!String.IsNullOrEmpty(alias.Value.SearchRouting))
                addAction.SearchRouting = alias.Value.SearchRouting;

            aliasActions.Add(new IndexUpdateAliasesAction
            {
                Add = addAction
            });
        }

        var response = await _client.Indices.UpdateAliasesAsync(x => x.Actions(aliasActions)).AnyContext();
        if (!response.IsValidResponse)
        {
            _logger.LogErrorRequest(response, "Error updating aliases during reindex");
            return false;
        }

        _logger.LogRequest(response);
        await progressCallbackAsync(92, $"Updated aliases: {String.Join(", ", aliases.Keys)} Remove: {oldIndex} Add: {newIndex}").AnyContext();
        return true;
    }

    private async Task<ReindexResult> InternalReindexAsync(ReindexWorkItem workItem, Func<int, string?, Task> progressCallbackAsync, int startProgress = 0, int endProgress = 100, DateTime? startTime = null, CancellationToken cancellationToken = default)
    {
        var query = await GetResumeQueryAsync(workItem.NewIndex, workItem.TimestampField, startTime).AnyContext();

        var result = await _resiliencePolicy.ExecuteAsync(async ct =>
        {
            var response = await _client.ReindexAsync(d =>
            {
                d.Source(src =>
                {
                    src.Indices(workItem.OldIndex);
                    if (query != null)
                        src.Query(query);
                    if (workItem.ReindexBatchSize.HasValue)
                        src.Size(workItem.ReindexBatchSize.Value);
                });
                d.Dest(dest => dest.Index(workItem.NewIndex));
                d.Conflicts(Conflicts.Proceed);
                d.WaitForCompletion(false);

                if (workItem.ReindexRequestsPerSecond.HasValue)
                    d.RequestsPerSecond(workItem.ReindexRequestsPerSecond.Value);

                if (!String.IsNullOrWhiteSpace(workItem.Script))
                    d.Script(new Script { Source = workItem.Script });
            }, ct).AnyContext();
            _logger.LogRequest(response);

            return response;
        }, cancellationToken).AnyContext();

        if (result.Task is null)
        {
            _logger.LogError("Reindex failed to start - no task returned. Response valid: {IsValid}, Reason: {Reason}",
                result.IsValidResponse, result.ElasticsearchServerError?.Error?.Reason ?? "Unknown");
            _logger.LogErrorRequest(result, "Reindex failed");
            return new ReindexResult { Total = 0, Completed = 0 };
        }

        _logger.LogInformation("Reindex Task Id: {ReindexTaskId}", result.Task.FullyQualifiedId);
        _logger.LogRequest(result);
        long totalDocs = result.Total ?? 0;

        bool taskSuccess = false;
        TaskReindexResult? lastReindexResponse = null;
        int statusGetFails = 0;
        long lastProgress = 0;
        var noProgressTimeout = GetNoProgressTimeout(workItem);
        var sw = Stopwatch.StartNew();
        try
        {
            do
            {
                var status = await _client.Tasks.GetAsync(result.Task.FullyQualifiedId, cancellationToken).AnyContext();
                if (status.IsValidResponse)
                {
                    _logger.LogRequest(status);
                }
                else
                {
                    _logger.LogErrorRequest(status, "Error getting task status while reindexing: {OldIndex} -> {NewIndex}", workItem.OldIndex, workItem.NewIndex);
                    statusGetFails++;

                    if (statusGetFails > MAX_STATUS_FAILS)
                    {
                        _logger.LogError("Failed to get the status {FailureCount} times in a row for reindex task {ReindexTaskId} reindexing {OldIndex} -> {NewIndex}",
                            statusGetFails, result.Task.FullyQualifiedId, workItem.OldIndex, workItem.NewIndex);
                        break;
                    }

                    // Back off before retrying so a struggling cluster (e.g. rejecting requests due to
                    // indexing pressure) isn't hammered with an immediate retry.
                    await _timeProvider.Delay(GetStatusRetryDelay(statusGetFails), cancellationToken).AnyContext();
                    continue;
                }

                statusGetFails = 0;

                var response = status.DeserializeRaw<TaskWithReindexResponse>(_serializer);
                if (response?.Error != null)
                {
                    _logger.LogError("Error reindex: {Type}, {Reason}, Cause: {CausedBy} Stack: {Stack}", response.Error.Type, response.Error.Reason, response.Error.Caused_By?.Reason, String.Join("\r\n", response.Error.Script_Stack ?? new List<string>()));
                    break;
                }

                lastReindexResponse = response?.Response;

                // Extract status values from the raw JSON. The Status property is object? and may be
                // deserialized as JsonElement or IDictionary<string, object> depending on serializer config.
                TaskStatusValues? taskStatus = null;
                if (status.Task.Status is JsonElement jsonElement)
                {
                    taskStatus = new TaskStatusValues
                    {
                        Total = jsonElement.TryGetProperty("total", out var totalProp) ? totalProp.GetInt64() : 0,
                        Created = jsonElement.TryGetProperty("created", out var createdProp) ? createdProp.GetInt64() : 0,
                        Updated = jsonElement.TryGetProperty("updated", out var updatedProp) ? updatedProp.GetInt64() : 0,
                        Noops = jsonElement.TryGetProperty("noops", out var noopsProp) ? noopsProp.GetInt64() : 0,
                        VersionConflicts = jsonElement.TryGetProperty("version_conflicts", out var conflictsProp) ? conflictsProp.GetInt64() : 0
                    };
                }
                else if (status.Task.Status is IDictionary<string, object> dict)
                {
                    taskStatus = new TaskStatusValues
                    {
                        Total = dict.TryGetValue("total", out var totalVal) ? Convert.ToInt64(totalVal) : 0,
                        Created = dict.TryGetValue("created", out var createdVal) ? Convert.ToInt64(createdVal) : 0,
                        Updated = dict.TryGetValue("updated", out var updatedVal) ? Convert.ToInt64(updatedVal) : 0,
                        Noops = dict.TryGetValue("noops", out var noopsVal) ? Convert.ToInt64(noopsVal) : 0,
                        VersionConflicts = dict.TryGetValue("version_conflicts", out var conflictsVal) ? Convert.ToInt64(conflictsVal) : 0
                    };
                }
                else if (status.Task.Status != null)
                {
                    _logger.LogWarning("Unexpected task status type {StatusType}: {Status}", status.Task.Status.GetType().Name, status.Task.Status);
                }

                long lastCompleted = (taskStatus?.Created ?? 0) + (taskStatus?.Updated ?? 0) + (taskStatus?.Noops ?? 0);

                // restart the stop watch if there was progress made
                if (lastCompleted > lastProgress)
                    sw.Restart();
                lastProgress = lastCompleted;

                string lastMessage = $"[{workItem.NewIndex}] Total: {taskStatus?.Total:N0} Completed: {lastCompleted:N0} VersionConflicts: {taskStatus?.VersionConflicts:N0}";
                await progressCallbackAsync(CalculateProgress(taskStatus?.Total ?? 0, lastCompleted, startProgress, endProgress), lastMessage).AnyContext();

                if (status.Completed && response?.Error == null)
                {
                    taskSuccess = true;
                    break;
                }

                // waited longer than noProgressTimeout (extended beyond the 10 minute default when
                // ReindexRequestsPerSecond makes Elasticsearch's own inter-batch pause longer than that) with
                // no progress made
                if (sw.Elapsed > noProgressTimeout)
                {
                    _logger.LogError("Timed out waiting for reindex {OldIndex} -> {NewIndex}. NoProgressTimeout: {NoProgressTimeout}", workItem.OldIndex, workItem.NewIndex, noProgressTimeout);
                    break;
                }

                var timeToWait = TimeSpan.FromSeconds(totalDocs < 100000 ? 1 : 10);
                if ((taskStatus?.Total ?? 0) < 100)
                    timeToWait = TimeSpan.FromMilliseconds(100);

                await _timeProvider.Delay(timeToWait, cancellationToken).AnyContext();
            } while (!cancellationToken.IsCancellationRequested);
        }
        catch (Exception taskException)
        {
            sw.Stop();
            await ElasticReindexTaskCancellation.CancelAndConfirmAsync(
                _client,
                _logger,
                result.Task,
                workItem.OldIndex,
                workItem.NewIndex,
                taskException).AnyContext();
            throw;
        }
        sw.Stop();

        if (!taskSuccess)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                _logger.LogWarning("Reindex cancelled for {OldIndex} -> {NewIndex}. ReindexTaskId: {ReindexTaskId}, LastProgress: {LastProgress}, TotalDocs: {TotalDocs}, Elapsed: {Elapsed}",
                    workItem.OldIndex, workItem.NewIndex, result.Task.FullyQualifiedId, lastProgress, totalDocs, sw.Elapsed);
            }
            else
            {
                _logger.LogError("Reindex abandoned for {OldIndex} -> {NewIndex}. ReindexTaskId: {ReindexTaskId}, StatusFails: {StatusFails}, LastProgress: {LastProgress}, TotalDocs: {TotalDocs}, Elapsed: {Elapsed}",
                    workItem.OldIndex, workItem.NewIndex, result.Task.FullyQualifiedId, statusGetFails, lastProgress, totalDocs, sw.Elapsed);
            }

            var taskException = new RepositoryException($"Reindex task '{result.Task.FullyQualifiedId}' did not complete successfully.");
            await ElasticReindexTaskCancellation.CancelAndConfirmAsync(
                _client,
                _logger,
                result.Task,
                workItem.OldIndex,
                workItem.NewIndex,
                taskException).AnyContext();
        }

        long failures = 0;
        if (lastReindexResponse?.Failures != null && lastReindexResponse.Failures.Count > 0)
        {
            _logger.LogError("Error while reindexing result");

            if (await CreateFailureIndexAsync(workItem).AnyContext())
            {
                foreach (var failure in lastReindexResponse.Failures)
                {
                    await HandleFailureAsync(workItem, failure).AnyContext();
                    failures++;
                }
            }
            taskSuccess = false;
        }

        long total = lastReindexResponse?.Total ?? 0;
        long versionConflicts = lastReindexResponse?.VersionConflicts ?? 0;
        long completed = (lastReindexResponse?.Created ?? 0) + (lastReindexResponse?.Updated ?? 0) + (lastReindexResponse?.Noops ?? 0);
        string message = $"Total: {total:N0} Completed: {completed:N0} VersionConflicts: {versionConflicts:N0}";
        await progressCallbackAsync(CalculateProgress(total, completed, startProgress, endProgress), message).AnyContext();
        return new ReindexResult { Total = total, Completed = completed, Failures = failures, Succeeded = taskSuccess };
    }

    private async Task<bool> CreateFailureIndexAsync(ReindexWorkItem workItem)
    {
        string errorIndex = $"{workItem.NewIndex}-error";
        var existsResponse = await _client.Indices.ExistsAsync(errorIndex).AnyContext();
        _logger.LogRequest(existsResponse);

        if (existsResponse.ApiCallDetails.HasSuccessfulStatusCode && existsResponse.Exists)
            return true;

        if (!existsResponse.ApiCallDetails.HasSuccessfulStatusCode && existsResponse.ApiCallDetails.HttpStatusCode is not 404)
        {
            _logger.LogErrorRequest(existsResponse, "Error checking if error index exists");
            return false;
        }

        var createResponse = await _client.Indices.CreateAsync(errorIndex, d => d.Mappings(md => md.Dynamic(DynamicMapping.False))).AnyContext();
        if (!createResponse.IsValidResponse)
        {
            _logger.LogErrorRequest(createResponse, "Unable to create error index");
            return false;
        }

        _logger.LogRequest(createResponse);
        return true;
    }

    private async Task HandleFailureAsync(ReindexWorkItem workItem, BulkIndexByScrollFailure failure)
    {
        _logger.LogError("Error reindexing document {Index}/{Id}: [{Status}] {Message}", workItem.OldIndex, failure.Id, failure.Status, failure.Cause?.Reason);

        if (String.IsNullOrEmpty(failure.Id))
        {
            _logger.LogWarning("Skipping error document fetch: failure has no document Id");
            return;
        }

        var gr = await _client.GetAsync<object>(request: new GetRequest(workItem.OldIndex, failure.Id)).AnyContext();

        if (!gr.IsValidResponse)
        {
            _logger.LogErrorRequest(gr, "Error getting document {Index}/{Id}", workItem.OldIndex, failure.Id);
            return;
        }

        _logger.LogRequest(gr);
        var errorDocument = new
        {
            failure.Index,
            failure.Id,
            gr.Version,
            gr.Routing,
            gr.Source,
            Cause = new
            {
                Type = failure.Cause?.Type,
                Reason = failure.Cause?.Reason,
                StackTrace = failure.Cause?.StackTrace
            },
            failure.Status,
            gr.Found,
        };
        var indexResponse = await _client.IndexAsync(errorDocument, i => i.Index($"{workItem.NewIndex}-error"));
        if (indexResponse.IsValidResponse)
            _logger.LogRequest(indexResponse);
        else
            _logger.LogErrorRequest(indexResponse, "Error indexing document {Index}/{Id}", $"{workItem.NewIndex}-error", gr.Id);
    }

    private async Task<string> ResolveConcreteIndexAsync(string name)
    {
        var response = await _client.Indices.GetAsync((Indices)name, d => d.LimitToNamesAndAliases()).AnyContext();
        if (!response.IsValidResponse)
            throw new InvalidOperationException(response.GetErrorMessage($"Unable to resolve source index '{name}'"), response.OriginalException());

        if (response.Indices is null || response.Indices.Count is 0)
            throw new InvalidOperationException($"Source index '{name}' did not resolve to a concrete index.");

        if (response.Indices.Count > 1)
            throw new InvalidOperationException($"Source index '{name}' resolved to multiple concrete indexes. Reindex work items must identify exactly one source index.");

        return response.Indices.Keys.Single().ToString();
    }

    private async Task<Dictionary<string, AliasDefinition>> GetIndexAliasesAsync(string index)
    {
        var aliasesResponse = await _client.Indices.GetAliasAsync(Indices.Index(index)).AnyContext();
        _logger.LogRequest(aliasesResponse);

        if (aliasesResponse.IsValidResponse)
        {
#if ELASTICSEARCH9
            var indices = aliasesResponse.Aliases;
#else
            var indices = aliasesResponse.Values;
#endif
            if (indices != null && indices.Count > 0)
            {
                var aliases = indices.SingleOrDefault(a => String.Equals(a.Key, index));
                if (aliases.Value?.Aliases != null)
                    return aliases.Value.Aliases.ToDictionary(a => a.Key.ToString(), a => a.Value, StringComparer.Ordinal);
            }

            return [];
        }

        if (aliasesResponse.ApiCallDetails is { HttpStatusCode: 404 })
            return [];

        _logger.LogWarning("Failed to get aliases for index {Index}: {Error}", index,
            aliasesResponse.ElasticsearchServerError?.Error?.Reason ?? "Unknown error");

        return [];
    }

    private async Task<Query?> GetResumeQueryAsync(string newIndex, string? timestampField, DateTime? startTime)
    {
        var descriptor = new QueryDescriptor<object>();
        if (startTime.HasValue)
            return CreateRangeQuery(descriptor, timestampField, startTime);

        var startingPoint = await GetResumeStartingPointAsync(newIndex, timestampField ?? ID_FIELD).AnyContext();
        if (startingPoint.HasValue)
            return CreateRangeQuery(descriptor, timestampField, startingPoint);

        // Return null when no query is needed - reindexing all documents
        return null;
    }

    private Query? CreateRangeQuery(QueryDescriptor<object> descriptor, string? timestampField, DateTime? startTime)
    {
        if (!startTime.HasValue)
            return descriptor;

        var start = startTime.Value;

        if (!String.IsNullOrEmpty(timestampField))
            return descriptor.Range(dr => dr.Date(drr => drr.Field(timestampField).Gte(start)));

        return descriptor.Range(dr => dr.Term(tr => tr.Field(ID_FIELD).Gte(ObjectId.GenerateNewId(start).ToString())));
    }

    private async Task<DateTime?> GetResumeStartingPointAsync(string newIndex, string timestampField)
    {
        var newestDocumentResponse = await _client.SearchAsync<IDictionary<string, object>>(d => d
            .Indices(newIndex)
            .Sort(s => s.Field(timestampField, fs => fs.Order(SortOrder.Desc)))
            .DocvalueFields(new FieldAndFormat[] { new() { Field = timestampField } })
            .Source(new SourceConfig(false))
            .Size(1)
        ).AnyContext();

        _logger.LogRequest(newestDocumentResponse);
        if (!newestDocumentResponse.IsValidResponse || !newestDocumentResponse.Documents.Any())
            return null;

        var doc = newestDocumentResponse.Hits.FirstOrDefault();
        if (doc == null)
            return null;

        if (timestampField == ID_FIELD)
        {
            if (!ObjectId.TryParse(doc.Id, out var objectId))
                return null;

            return objectId.CreationTime;
        }

        var value = doc.Fields?[timestampField];
        if (value == null)
            return null;

        if (value is not JsonElement jsonElement)
            return null;

        var target = jsonElement;
        if (jsonElement.ValueKind == JsonValueKind.Array)
        {
            if (jsonElement.GetArrayLength() == 0)
                return null;
            target = jsonElement[0];
        }

        if (target.TryGetDateTime(out var dateTime))
            return dateTime;
        if (target.ValueKind == JsonValueKind.String && DateTime.TryParse(target.GetString(), out dateTime))
            return dateTime;

        return null;
    }

    private enum SampleIdStatus { Found, Empty, Failed }

    private sealed record SampleIdResult(SampleIdStatus Status, string? Id = null, string? Error = null, Exception? Exception = null);

    private async Task<SampleIdResult> GetSampleDocumentIdAsync(string index)
    {
        var response = await _client.SearchAsync<IDictionary<string, object>>(d => d
            .Indices(index)
            .Source(new SourceConfig(false))
            .Size(1)
        ).AnyContext();

        _logger.LogRequest(response);

        if (!response.IsValidResponse)
            return new SampleIdResult(SampleIdStatus.Failed, Error: response.GetErrorMessage("Search failed"), Exception: response.OriginalException());

        if (!response.Hits.Any())
            return new SampleIdResult(SampleIdStatus.Empty);

        return new SampleIdResult(SampleIdStatus.Found, Id: response.Hits.First().Id);
    }

    private int CalculateProgress(long total, long completed, int startProgress = 0, int endProgress = 100)
    {
        if (total == 0) return startProgress;
        return startProgress + (int)((100 * (double)completed / total) * (((double)endProgress - startProgress) / 100));
    }

    private static readonly Func<int, TimeSpan> _statusRetryExponentialDelay = ResiliencePolicy.ExponentialDelay(TimeSpan.FromSeconds(1));
    private static readonly TimeSpan _maxStatusRetryDelay = TimeSpan.FromSeconds(30);

    // Any attempt count at or beyond this already saturates the exponential delay past _maxStatusRetryDelay
    // (2^(6-1) = 32s > 30s cap), so clamping here is purely overflow-safety headroom for Math.Pow, not a
    // behavioral limit. It intentionally isn't tied to MAX_STATUS_FAILS - those are separate concerns that
    // happen to share the same value today.
    private const int MaxAttemptsForDelayCalculation = 10;

    /// <summary>
    /// Computes the backoff delay before retrying a failed task status check, e.g. after Elasticsearch
    /// rejects the request due to indexing pressure (HTTP 429). Grows exponentially starting at 1 second,
    /// caps at 30 seconds so repeated failures don't hammer a struggling cluster, and applies +/-25% jitter
    /// (matching <see cref="ResiliencePolicy"/>'s own jitter formula) so multiple reindex operations failing
    /// at the same time due to a cluster-wide condition don't retry in lockstep.
    /// </summary>
    internal static TimeSpan GetStatusRetryDelay(int failedAttempts)
    {
        int clampedAttempts = Math.Clamp(failedAttempts, 1, MaxAttemptsForDelayCalculation);
        var delay = _statusRetryExponentialDelay(clampedAttempts);

        double offset = delay.TotalMilliseconds * 0.25;
        double jitteredMilliseconds = delay.TotalMilliseconds + (delay.TotalMilliseconds * 0.5 * Random.Shared.NextDouble() - offset);
        delay = TimeSpan.FromMilliseconds(Math.Max(0, jitteredMilliseconds));

        return delay > _maxStatusRetryDelay ? _maxStatusRetryDelay : delay;
    }

    private static readonly TimeSpan DefaultNoProgressTimeout = TimeSpan.FromMinutes(10);

    // Elasticsearch's own reindex API default for Source.Size when ReindexBatchSize isn't specified -
    // see https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-reindex.
    private const int DefaultElasticsearchBatchSize = 1000;

    // Elasticsearch pauses roughly batchSize/requestsPerSecond between batches to honor the throttle. This
    // multiplier gives that pause headroom (write time, network latency) before treating it as a stall.
    private const int NoProgressTimeoutSafetyMultiplier = 3;

    /// <summary>
    /// Computes how long to wait for progress before treating a reindex as stalled and abandoning it.
    /// Defaults to 10 minutes. When <see cref="ReindexWorkItem.ReindexRequestsPerSecond"/> throttles the
    /// reindex, Elasticsearch pauses between batches to honor that rate, and the pause can exceed the
    /// default timeout for a low configured rate relative to the batch size. In that case the timeout is
    /// extended (with a safety margin) so a healthy, intentionally throttled reindex isn't mistaken for a
    /// stalled one and cancelled. The result is clamped to <see cref="TimeSpan.MaxValue"/> instead of
    /// overflowing for extreme (but otherwise valid) batch size/throttle combinations, such as an unbounded
    /// <see cref="ReindexWorkItem.ReindexBatchSize"/> paired with a very low
    /// <see cref="ReindexWorkItem.ReindexRequestsPerSecond"/>.
    /// </summary>
    internal static TimeSpan GetNoProgressTimeout(ReindexWorkItem workItem)
    {
        if (workItem.ReindexRequestsPerSecond is not > 0)
            return DefaultNoProgressTimeout;

        int effectiveBatchSize = workItem.ReindexBatchSize is > 0 ? workItem.ReindexBatchSize.Value : DefaultElasticsearchBatchSize;

        // Computed entirely in double (seconds) space and clamped before constructing a TimeSpan - an
        // extreme batch size/throttle combination (e.g. a very large ReindexBatchSize with a tiny
        // ReindexRequestsPerSecond) can otherwise overflow TimeSpan's ~29,000 year range and throw.
        double throttledTimeoutSeconds = effectiveBatchSize / (double)workItem.ReindexRequestsPerSecond.Value * NoProgressTimeoutSafetyMultiplier;
        if (throttledTimeoutSeconds >= TimeSpan.MaxValue.TotalSeconds)
            return TimeSpan.MaxValue;

        var throttledTimeout = TimeSpan.FromSeconds(throttledTimeoutSeconds);
        return throttledTimeout > DefaultNoProgressTimeout ? throttledTimeout : DefaultNoProgressTimeout;
    }

    private record ReindexResult
    {
        public long Total { get; init; }
        public long Completed { get; init; }
        public long Failures { get; init; }
        public bool Succeeded { get; init; }
    }

    private record TaskWithReindexResponse
    {
        public TaskReindexResult? Response { get; init; }
        public TaskReindexError? Error { get; init; }
    }

    private record TaskReindexError
    {
        public string? Type { get; init; }
        public string? Reason { get; init; }
        public List<string>? Script_Stack { get; init; }

        public TaskCause? Caused_By { get; init; }
    }

    private record TaskCause
    {
        public string? Type { get; init; }
        public string? Reason { get; init; }
    }

    private record TaskReindexResult
    {
        public long Total { get; init; }
        public long Created { get; init; }
        public long Updated { get; init; }
        public long Noops { get; init; }
        public long VersionConflicts { get; init; }

        public IReadOnlyCollection<BulkIndexByScrollFailure>? Failures { get; init; }
    }

    private record TaskStatusValues
    {
        public long Total { get; init; }
        public long Created { get; init; }
        public long Updated { get; init; }
        public long Noops { get; init; }
        public long VersionConflicts { get; init; }
    }

    private record BulkIndexByScrollFailure
    {
        public Error? Cause { get; init; }
        public string? Id { get; init; }
        public string? Index { get; init; }
        public int Status { get; init; }
        public string? Type { get; init; }
    }
}
