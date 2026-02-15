using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Elasticsearch.Net;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Jobs;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Utility;
using Foundatio.Resilience;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Nest;
using Newtonsoft.Json;

namespace Foundatio.Repositories.Elasticsearch;

public class ElasticReindexer
{
    private readonly IElasticClient _client;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger _logger;
    private readonly IResiliencePolicyProvider _resiliencePolicyProvider;
    private readonly IResiliencePolicy _resiliencePolicy;
    private const string ID_FIELD = "id";
    private const int MAX_STATUS_FAILS = 10;

    public ElasticReindexer(IElasticClient client, ILogger logger = null) : this(client, TimeProvider.System, logger)
    {
    }

    public ElasticReindexer(IElasticClient client, TimeProvider timeProvider, ILogger logger = null) : this(client, timeProvider ?? TimeProvider.System, new ResiliencePolicyProvider(), logger ?? NullLogger.Instance)
    {
    }

    public ElasticReindexer(IElasticClient client, TimeProvider timeProvider, IResiliencePolicyProvider resiliencePolicyProvider, ILogger logger = null)
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
                var bulkResponse = await _client.Indices.BulkAliasAsync(x =>
                {
                    foreach (string alias in aliases)
                        x = x.Remove(a => a.Alias(alias).Index(workItem.OldIndex)).Add(a => a.Alias(alias).Index(workItem.NewIndex));

                    return x;
                }).AnyContext();
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

            var newDocCountResponse = await _client.CountAsync<object>(d => d.Index(workItem.NewIndex)).AnyContext();
            _logger.LogRequest(newDocCountResponse);

            var oldDocCountResponse = await _client.CountAsync<object>(d => d.Index(workItem.OldIndex)).AnyContext();
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
            var response = await _client.ReindexOnServerAsync(d =>
            {
                d.Source(src => src
                    .Index(workItem.OldIndex)
                    .Query<object>(q => query))
                .Destination(dest => dest.Index(workItem.NewIndex))
                .Conflicts(Conflicts.Proceed)
                .WaitForCompletion(false);

                // NEST client emitting a script if null, inline this when that's fixed
                if (!String.IsNullOrWhiteSpace(workItem.Script))
                    d.Script(workItem.Script);

                return d;
            }, ct).AnyContext();
            _logger.LogRequest(response);

            return response;
        }, cancellationToken).AnyContext();

        _logger.LogInformation("Reindex Task Id: {TaskId}", result.Task.FullyQualifiedId);
        _logger.LogRequest(result);
        long totalDocs = result.Total;

        bool taskSuccess = false;
        TaskReindexResult lastReindexResponse = null;
        int statusGetFails = 0;
        long lastProgress = 0;
        var sw = Stopwatch.StartNew();
        do
        {
            var status = await _client.Tasks.GetTaskAsync(result.Task, null, cancellationToken).AnyContext();
            if (status.IsValid)
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

            long lastCompleted = status.Task.Status.Created + status.Task.Status.Updated + status.Task.Status.Noops;

            // restart the stop watch if there was progress made
            if (lastCompleted > lastProgress)
                sw.Restart();
            lastProgress = lastCompleted;

            string lastMessage = $"Total: {status.Task.Status.Total:N0} Completed: {lastCompleted:N0} VersionConflicts: {status.Task.Status.VersionConflicts:N0}";
            await progressCallbackAsync(CalculateProgress(status.Task.Status.Total, lastCompleted, startProgress, endProgress), lastMessage).AnyContext();

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
            if (status.Task.Status.Total < 100)
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
        string errorIndex = workItem.NewIndex + "-error";
        var existsResponse = await _client.Indices.ExistsAsync(errorIndex).AnyContext();
        _logger.LogRequest(existsResponse);
        if (existsResponse.ApiCall.Success && existsResponse.Exists)
            return true;

        var createResponse = await _client.Indices.CreateAsync(errorIndex, d => d.Map(md => md.Dynamic(false))).AnyContext();
        if (!createResponse.IsValid)
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

        if (!gr.IsValid)
        {
            _logger.LogErrorRequest(gr, "Error getting document {Index}/{Id}", workItem.OldIndex, failure.Id);
            return;
        }

        _logger.LogRequest(gr);
        string document = JsonConvert.SerializeObject(new
        {
            failure.Index,
            failure.Id,
            gr.Version,
            gr.Routing,
            gr.Source,
            failure.Cause,
            failure.Status,
            gr.Found,
        });
        var indexResponse = await _client.LowLevel.IndexAsync<VoidResponse>(workItem.NewIndex + "-error", PostData.String(document));
        if (indexResponse.Success)
            _logger.LogRequest(indexResponse);
        else
            _logger.LogErrorRequest(indexResponse, "Error indexing document {Index}/{Id}", workItem.NewIndex + "-error", gr.Id);
    }

    private async Task<List<string>> GetIndexAliasesAsync(string index)
    {
        var aliasesResponse = await _client.Indices.GetAliasAsync(index).AnyContext();
        _logger.LogRequest(aliasesResponse);

        if (aliasesResponse.IsValid && aliasesResponse.Indices.Count > 0)
        {
            var aliases = aliasesResponse.Indices.Single(a => a.Key == index);
            return aliases.Value.Aliases.Select(a => a.Key).ToList();
        }

        return new List<string>();
    }

    private async Task<QueryContainer> GetResumeQueryAsync(string newIndex, string timestampField, DateTime? startTime)
    {
        var descriptor = new QueryContainerDescriptor<object>();
        if (startTime.HasValue)
            return CreateRangeQuery(descriptor, timestampField, startTime);

        var startingPoint = await GetResumeStartingPointAsync(newIndex, timestampField ?? ID_FIELD).AnyContext();
        if (startingPoint.HasValue)
            return CreateRangeQuery(descriptor, timestampField, startingPoint);

        return descriptor;
    }

    private QueryContainer CreateRangeQuery(QueryContainerDescriptor<object> descriptor, string timestampField, DateTime? startTime)
    {
        if (!startTime.HasValue)
            return descriptor;

        if (!String.IsNullOrEmpty(timestampField))
            return descriptor.DateRange(dr => dr.Field(timestampField).GreaterThanOrEquals(startTime));

        return descriptor.TermRange(dr => dr.Field(ID_FIELD).GreaterThanOrEquals(ObjectId.GenerateNewId(startTime.Value).ToString()));
    }

    private async Task<DateTime?> GetResumeStartingPointAsync(string newIndex, string timestampField)
    {
        var newestDocumentResponse = await _client.SearchAsync<IDictionary<string, object>>(d => d
            .Index(newIndex)
            .Sort(s => s.Descending(timestampField))
            .DocValueFields(timestampField)
            .Source(s => s.ExcludeAll())
            .Size(1)
        ).AnyContext();

        _logger.LogRequest(newestDocumentResponse);
        if (!newestDocumentResponse.IsValid || !newestDocumentResponse.Documents.Any())
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

        var datesArray = await value.AsAsync<DateTime[]>();
        return datesArray?.FirstOrDefault();
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

    private class BulkIndexByScrollFailure
    {
        public Error Cause { get; set; }
        public string Id { get; set; }
        public string Index { get; set; }
        public int Status { get; set; }
        public string Type { get; set; }
    }
}
