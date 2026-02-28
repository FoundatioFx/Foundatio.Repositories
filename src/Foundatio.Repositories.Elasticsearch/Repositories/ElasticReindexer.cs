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
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Jobs;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Utility;
using Foundatio.Resilience;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Foundatio.Repositories.Elasticsearch;

public class ElasticReindexer
{
    private readonly ElasticsearchClient _client;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger _logger;
    private readonly IResiliencePolicyProvider _resiliencePolicyProvider;
    private readonly IResiliencePolicy _resiliencePolicy;
    private const string ID_FIELD = "id";
    private const int MAX_STATUS_FAILS = 10;

    public ElasticReindexer(ElasticsearchClient client, ILogger logger = null) : this(client, TimeProvider.System, logger)
    {
    }

    public ElasticReindexer(ElasticsearchClient client, TimeProvider timeProvider, ILogger logger = null) : this(client, timeProvider ?? TimeProvider.System, new ResiliencePolicyProvider(), logger ?? NullLogger.Instance)
    {
    }

    public ElasticReindexer(ElasticsearchClient client, TimeProvider timeProvider, IResiliencePolicyProvider resiliencePolicyProvider, ILogger logger = null)
    {
        _client = client;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _resiliencePolicyProvider = resiliencePolicyProvider ?? new ResiliencePolicyProvider();
        _logger = logger ?? NullLogger.Instance;

        _resiliencePolicy = _resiliencePolicyProvider.GetPolicy<ElasticReindexer>(fallback => fallback.WithMaxAttempts(5).WithDelay(TimeSpan.FromSeconds(10)), _logger, _timeProvider);
    }

    public async Task ReindexAsync(ReindexWorkItem workItem, Func<int, string, Task> progressCallbackAsync = null)
    {
        if (String.IsNullOrEmpty(workItem.OldIndex))
            throw new ArgumentNullException(nameof(workItem.OldIndex));

        if (String.IsNullOrEmpty(workItem.NewIndex))
            throw new ArgumentNullException(nameof(workItem.NewIndex));

        if (progressCallbackAsync == null)
        {
            progressCallbackAsync = (progress, message) =>
            {
                _logger.LogInformation("Reindex Progress {Progress:F1}%: {Message}", progress, message);
                return Task.CompletedTask;
            };
        }

        _logger.LogInformation("Received reindex work item for new index: {NewIndex}", workItem.NewIndex);
        var startTime = _timeProvider.GetUtcNow().UtcDateTime.AddSeconds(-1);
        await progressCallbackAsync(0, "Starting reindex...").AnyContext();
        var firstPassResult = await InternalReindexAsync(workItem, progressCallbackAsync, 0, 90, workItem.StartUtc).AnyContext();

        if (!firstPassResult.Succeeded) return;

        await progressCallbackAsync(91, $"Total: {firstPassResult.Total:N0} Completed: {firstPassResult.Completed:N0}").AnyContext();

        // TODO: Check to make sure the docs have been added to the new index before changing alias
        if (workItem.OldIndex != workItem.NewIndex)
        {
            var aliases = await GetIndexAliasesAsync(workItem.OldIndex).AnyContext();
            if (!String.IsNullOrEmpty(workItem.Alias) && !aliases.Contains(workItem.Alias))
                aliases.Add(workItem.Alias);

            if (aliases.Count > 0)
            {
                // Build list of actions - each action is either an Add or Remove
                var aliasActions = new List<IndexUpdateAliasesAction>();

                foreach (string alias in aliases)
                {
                    // Remove from old index
                    aliasActions.Add(new IndexUpdateAliasesAction { Remove = new RemoveAction { Alias = alias, Index = workItem.OldIndex } });
                    // Add to new index
                    aliasActions.Add(new IndexUpdateAliasesAction { Add = new AddAction { Alias = alias, Index = workItem.NewIndex } });
                }

                var bulkResponse = await _client.Indices.UpdateAliasesAsync(x => x.Actions(aliasActions)).AnyContext();

                if (!bulkResponse.IsValidResponse)
                {
                    _logger.LogErrorRequest(bulkResponse, "Error updating aliases during reindex");
                    return;
                }

                _logger.LogRequest(bulkResponse);

                await progressCallbackAsync(92, $"Updated aliases: {String.Join(", ", aliases)} Remove: {workItem.OldIndex} Add: {workItem.NewIndex}").AnyContext();
            }
        }

        var refreshResponse = await _client.Indices.RefreshAsync(Indices.All).AnyContext();
        _logger.LogRequest(refreshResponse);

        ReindexResult secondPassResult = null;
        if (!String.IsNullOrEmpty(workItem.TimestampField))
        {
            secondPassResult = await InternalReindexAsync(workItem, progressCallbackAsync, 92, 96, startTime).AnyContext();
            if (!secondPassResult.Succeeded) return;

            await progressCallbackAsync(97, $"Total: {secondPassResult.Total:N0} Completed: {secondPassResult.Completed:N0}").AnyContext();
        }

        long totalFailures = firstPassResult.Failures;
        if (secondPassResult != null)
            totalFailures += secondPassResult.Failures;

        bool hasFailures = totalFailures > 0;
        if (!hasFailures && workItem.DeleteOld && workItem.OldIndex != workItem.NewIndex)
        {
            refreshResponse = await _client.Indices.RefreshAsync(Indices.All).AnyContext();
            _logger.LogRequest(refreshResponse);

            var newDocCountResponse = await _client.CountAsync<object>(d => d.Indices(workItem.NewIndex)).AnyContext();
            _logger.LogRequest(newDocCountResponse);

            var oldDocCountResponse = await _client.CountAsync<object>(d => d.Indices(workItem.OldIndex)).AnyContext();
            _logger.LogRequest(oldDocCountResponse);

            await progressCallbackAsync(98, $"Old Docs: {oldDocCountResponse.Count} New Docs: {newDocCountResponse.Count}").AnyContext();
            if (newDocCountResponse.Count >= oldDocCountResponse.Count)
            {
                var deleteIndexResponse = await _client.Indices.DeleteAsync(Indices.Index(workItem.OldIndex)).AnyContext();
                _logger.LogRequest(deleteIndexResponse);

                await progressCallbackAsync(99, $"Deleted index: {workItem.OldIndex}").AnyContext();
            }
        }

        await progressCallbackAsync(100, null).AnyContext();
    }

    private async Task<ReindexResult> InternalReindexAsync(ReindexWorkItem workItem, Func<int, string, Task> progressCallbackAsync, int startProgress = 0, int endProgress = 100, DateTime? startTime = null, CancellationToken cancellationToken = default)
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
                });
                d.Dest(dest => dest.Index(workItem.NewIndex));
                d.Conflicts(Conflicts.Proceed);
                d.WaitForCompletion(false);

                if (!String.IsNullOrWhiteSpace(workItem.Script))
                    d.Script(new Script { Source = workItem.Script });
            }, ct).AnyContext();
            _logger.LogRequest(response);

            return response;
        }, cancellationToken).AnyContext();

        if (result.Task == null)
        {
            _logger.LogError("Reindex failed to start - no task returned. Response valid: {IsValid}, Error: {Error}",
                result.IsValidResponse, result.ElasticsearchServerError?.Error?.Reason ?? "Unknown");
            _logger.LogErrorRequest(result, "Reindex failed");
            return new ReindexResult { Total = 0, Completed = 0 };
        }

        _logger.LogInformation("Reindex Task Id: {TaskId}", result.Task.ToString());
        _logger.LogRequest(result);
        long totalDocs = result.Total ?? 0;

        bool taskSuccess = false;
        TaskReindexResult lastReindexResponse = null;
        int statusGetFails = 0;
        long lastProgress = 0;
        var sw = Stopwatch.StartNew();
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
                    _logger.LogError("Failed to get the status {FailureCount} times in a row", MAX_STATUS_FAILS);
                    break;
                }

                continue;
            }

            statusGetFails = 0;

            var response = status.DeserializeRaw<TaskWithReindexResponse>();
            if (response?.Error != null)
            {
                _logger.LogError("Error reindex: {Type}, {Reason}, Cause: {CausedBy} Stack: {Stack}", response.Error.Type, response.Error.Reason, response.Error.Caused_By?.Reason, String.Join("\r\n", response.Error.Script_Stack ?? new List<string>()));
                break;
            }

            lastReindexResponse = response?.Response;

            // Extract status values from the raw JSON. The Status property is object? and gets deserialized as JsonElement
            TaskStatusValues taskStatus = null;
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
            else if (status.Task.Status != null)
            {
                _logger.LogWarning("Unexpected task status type {StatusType}: {Status}", status.Task.Status.GetType().Name, status.Task.Status);
            }

            long lastCompleted = (taskStatus?.Created ?? 0) + (taskStatus?.Updated ?? 0) + (taskStatus?.Noops ?? 0);

            // restart the stop watch if there was progress made
            if (lastCompleted > lastProgress)
                sw.Restart();
            lastProgress = lastCompleted;

            string lastMessage = $"Total: {taskStatus?.Total:N0} Completed: {lastCompleted:N0} VersionConflicts: {taskStatus?.VersionConflicts:N0}";
            await progressCallbackAsync(CalculateProgress(taskStatus?.Total ?? 0, lastCompleted, startProgress, endProgress), lastMessage).AnyContext();

            if (status.Completed && response?.Error == null)
            {
                taskSuccess = true;
                break;
            }

            // waited more than 10 minutes with no progress made
            if (sw.Elapsed > TimeSpan.FromMinutes(10))
            {
                _logger.LogError("Timed out waiting for reindex {OldIndex} -> {NewIndex}", workItem.OldIndex, workItem.NewIndex);
                break;
            }

            var timeToWait = TimeSpan.FromSeconds(totalDocs < 100000 ? 1 : 10);
            if ((taskStatus?.Total ?? 0) < 100)
                timeToWait = TimeSpan.FromMilliseconds(100);

            await _timeProvider.Delay(timeToWait, cancellationToken).AnyContext();
        } while (!cancellationToken.IsCancellationRequested);
        sw.Stop();

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
        _logger.LogError("Error reindexing document {Index}/{Id}: [{Status}] {Message}", workItem.OldIndex, failure.Id, failure.Status, failure.Cause.Reason);
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

    private async Task<List<string>> GetIndexAliasesAsync(string index)
    {
        var aliasesResponse = await _client.Indices.GetAliasAsync(Indices.Index(index)).AnyContext();
        _logger.LogRequest(aliasesResponse);

        var indices = aliasesResponse.Aliases;
        if (aliasesResponse.IsValidResponse && indices != null && indices.Count > 0)
        {
            var aliases = indices.Single(a => a.Key == index);
            return aliases.Value.Aliases.Select(a => a.Key).ToList();
        }

        return new List<string>();
    }

    private async Task<Query> GetResumeQueryAsync(string newIndex, string timestampField, DateTime? startTime)
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

    private Query CreateRangeQuery(QueryDescriptor<object> descriptor, string timestampField, DateTime? startTime)
    {
        if (!startTime.HasValue)
            return descriptor;

        if (!String.IsNullOrEmpty(timestampField))
            return descriptor.Range(dr => dr.Date(drr => drr.Field(timestampField).Gte(startTime)));

        return descriptor.Range(dr => dr.Term(tr => tr.Field(ID_FIELD).Gte(ObjectId.GenerateNewId(startTime.GetValueOrDefault()).ToString())));
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

        // In the new Elastic client, field values are typically JsonElement objects
        if (value is JsonElement jsonElement)
        {
            if (jsonElement.ValueKind == JsonValueKind.Array && jsonElement.GetArrayLength() > 0)
            {
                var firstElement = jsonElement[0];
                if (firstElement.TryGetDateTime(out var dateTime))
                    return dateTime;
                // Try parsing as string if direct DateTime conversion fails
                if (firstElement.ValueKind == JsonValueKind.String && DateTime.TryParse(firstElement.GetString(), out dateTime))
                    return dateTime;
            }
        }

        return null;
    }

    private int CalculateProgress(long total, long completed, int startProgress = 0, int endProgress = 100)
    {
        if (total == 0) return startProgress;
        return startProgress + (int)((100 * (double)completed / total) * (((double)endProgress - startProgress) / 100));
    }

    private class ReindexResult
    {
        public long Total { get; set; }
        public long Completed { get; set; }
        public long Failures { get; set; }
        public bool Succeeded { get; set; }
    }

    private class TaskWithReindexResponse
    {
        public TaskReindexResult Response { get; set; }
        public TaskReindexError Error { get; set; }
    }

    private class TaskReindexError
    {
        public string Type { get; set; }
        public string Reason { get; set; }
        public List<string> Script_Stack { get; set; }

        public TaskCause Caused_By { get; set; }
    }

    private class TaskCause
    {
        public string Type { get; set; }
        public string Reason { get; set; }
    }

    private class TaskReindexResult
    {
        public long Total { get; set; }
        public long Created { get; set; }
        public long Updated { get; set; }
        public long Noops { get; set; }
        public long VersionConflicts { get; set; }

        public IReadOnlyCollection<BulkIndexByScrollFailure> Failures { get; set; }
    }

    private class TaskStatusValues
    {
        public long Total { get; set; }
        public long Created { get; set; }
        public long Updated { get; set; }
        public long Noops { get; set; }
        public long VersionConflicts { get; set; }
    }

    private class BulkIndexByScrollFailure
    {
        public Error Cause { get; set; }
        public string Id { get; set; }
        public string Index { get; set; }
        public int Status { get; set; }
        public string Type { get; set; }
    }
}
