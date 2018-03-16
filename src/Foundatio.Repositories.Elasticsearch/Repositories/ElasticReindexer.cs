using Elasticsearch.Net;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Jobs;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Utility;
using Foundatio.Utility;
using Nest;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System.Threading;
using System.Text;
using Newtonsoft.Json;
using System.Diagnostics;
using Foundatio.Repositories.Elasticsearch.Extensions;

namespace Foundatio.Repositories.Elasticsearch {
    public class ElasticReindexer {
        private readonly IElasticClient _client;
        private readonly ILogger _logger;
        private const string ID_FIELD = "id";
        private const int MAX_STATUS_FAILS = 10;

        public ElasticReindexer(IElasticClient client, ILogger logger = null) {
            _client = client;
            _logger = logger ?? NullLogger.Instance;
        }

        public async Task ReindexAsync(ReindexWorkItem workItem, Func<int, string, Task> progressCallbackAsync = null) {
            if (String.IsNullOrEmpty(workItem.OldIndex))
                throw new ArgumentNullException(nameof(workItem.OldIndex));

            if (String.IsNullOrEmpty(workItem.NewIndex))
                throw new ArgumentNullException(nameof(workItem.NewIndex));

            if (progressCallbackAsync == null) {
                progressCallbackAsync = (progress, message) => {
                    _logger.LogInformation("Reindex Progress {Progress:F1}%: {Message}", progress, message);
                    return Task.CompletedTask;
                };
            }

            _logger.LogInformation("Received reindex work item for new index: {NewIndex}", workItem.NewIndex);
            var startTime = SystemClock.UtcNow.AddSeconds(-1);
            await progressCallbackAsync(0, "Starting reindex...").AnyContext();
            var firstPassResult = await InternalReindexAsync(workItem, progressCallbackAsync, 0, 90, workItem.StartUtc).AnyContext();

            if (!firstPassResult.Succeeded) return;

            await progressCallbackAsync(91, $"Total: {firstPassResult.Total:N0} Completed: {firstPassResult.Completed:N0}").AnyContext();

            // TODO: Check to make sure the docs have been added to the new index before changing alias
            if (workItem.OldIndex != workItem.NewIndex) {
                var aliases = await GetIndexAliasesAsync(workItem.OldIndex).AnyContext();
                if (!String.IsNullOrEmpty(workItem.Alias) && !aliases.Contains(workItem.Alias))
                    aliases.Add(workItem.Alias);

                if (aliases.Count > 0) {
                    var bulkResponse = await _client.AliasAsync(x => {
                        foreach (string alias in aliases)
                            x = x.Remove(a => a.Alias(alias).Index(workItem.OldIndex)).Add(a => a.Alias(alias).Index(workItem.NewIndex));

                        return x;
                    }).AnyContext();
                    if (_logger.IsEnabled(Microsoft.Extensions.Logging.LogLevel.Trace))
                        _logger.LogTrace(bulkResponse.GetRequest());

                    await progressCallbackAsync(92, $"Updated aliases: {String.Join(", ", aliases)} Remove: {workItem.OldIndex} Add: {workItem.NewIndex}").AnyContext();
                }
            }

            await _client.RefreshAsync(Indices.All).AnyContext();
            var secondPassResult = await InternalReindexAsync(workItem, progressCallbackAsync, 92, 96, startTime).AnyContext();
            if (!secondPassResult.Succeeded) return;

            await progressCallbackAsync(97, $"Total: {secondPassResult.Total:N0} Completed: {secondPassResult.Completed:N0}").AnyContext();

            bool hasFailures = (firstPassResult.Failures + secondPassResult.Failures) > 0;
            if (!hasFailures && workItem.DeleteOld && workItem.OldIndex != workItem.NewIndex) {
                await _client.RefreshAsync(Indices.All).AnyContext();
                long newDocCount = (await _client.CountAsync<object>(d => d.Index(workItem.NewIndex).AllTypes()).AnyContext()).Count;
                long oldDocCount = (await _client.CountAsync<object>(d => d.Index(workItem.OldIndex).AllTypes()).AnyContext()).Count;
                await progressCallbackAsync(98, $"Old Docs: {oldDocCount} New Docs: {newDocCount}").AnyContext();
                if (newDocCount >= oldDocCount) {
                    await _client.DeleteIndexAsync(Indices.Index(workItem.OldIndex)).AnyContext();
                    await progressCallbackAsync(99, $"Deleted index: {workItem.OldIndex}").AnyContext();
                }
            }

            await progressCallbackAsync(100, null).AnyContext();
        }

        private async Task<ReindexResult> InternalReindexAsync(ReindexWorkItem workItem, Func<int, string, Task> progressCallbackAsync, int startProgress = 0, int endProgress = 100, DateTime? startTime = null, CancellationToken cancellationToken = default) {
            var query = await GetResumeQueryAsync(workItem.NewIndex, workItem.TimestampField, startTime).AnyContext();

            var sw = Stopwatch.StartNew();
            var result = await Run.WithRetriesAsync(async () => {
                    var response = await _client.ReindexOnServerAsync(d => {
                        d.Source(src => src
                            .Index(workItem.OldIndex)
                            .Query<object>(q => query)
                            .Sort<object>(s => s.Ascending(new Field(workItem.TimestampField ?? ID_FIELD))))
                        .Destination(dest => dest.Index(workItem.NewIndex))
                        .Conflicts(Conflicts.Proceed)
                        .WaitForCompletion(false);

                        //NEST client emitting script if null, inline this when that's fixed
                        if (!String.IsNullOrWhiteSpace(workItem.Script)) d.Script(workItem.Script);

                        return d;
                    }).AnyContext();

                    return response;
                }, 5, TimeSpan.FromSeconds(10), cancellationToken, _logger).AnyContext();

            if (_logger.IsEnabled(Microsoft.Extensions.Logging.LogLevel.Trace))
                _logger.LogTrace(result.GetRequest());

            bool taskSuccess = false;
            TaskReindexResult lastReindexResponse = null;
            var statusGetFails = 0;
            do {
                await Task.Delay(TimeSpan.FromSeconds(10), cancellationToken).AnyContext();

                var status = await _client.GetTaskAsync(result.Task, null, cancellationToken).AnyContext();
                if (!status.IsValid) {
                    _logger.LogError($"Error getting task status while reindexing: {workItem.OldIndex} -> {workItem.NewIndex}. Reason: {status.GetErrorMessage()}");
                    statusGetFails++;

                    if (statusGetFails > MAX_STATUS_FAILS) {
                        _logger.LogError($"Failed to get the status {MAX_STATUS_FAILS} times in a row");
                        break;
                    }

                    continue;
                }

                statusGetFails = 0;

                var response = status.DeserializeRaw<TaskWithReindexResponse>();
                if (response != null) {
                    if (response.Error != null) {
                        _logger.LogError($"Error reindex: {response.Error.Type}");
                        break;
                    }

                    lastReindexResponse = response.Response;
                }

                long lastCompleted = status.Task.Status.Created + status.Task.Status.Updated + status.Task.Status.Noops;
                string lastMessage = $"Total: {status.Task.Status.Total:N0} Completed: {lastCompleted:N0} VersionConflicts: {status.Task.Status.VersionConflicts:N0}";
                await progressCallbackAsync(CalculateProgress(status.Task.Status.Total, lastCompleted, startProgress, endProgress), lastMessage).AnyContext();

                if (status.Completed) {
                    taskSuccess = true;
                    break;
                }

                if (sw.Elapsed > TimeSpan.FromHours(3)) {
                    _logger.LogError($"Timed out waiting for reindex {workItem.OldIndex} -> {workItem.NewIndex}.");
                    break;
                }
            } while (!cancellationToken.IsCancellationRequested);
            sw.Stop();

            long failures = 0;
            if (lastReindexResponse?.Failures != null && lastReindexResponse.Failures.Count > 0) {
                _logger.LogError("Error while reindexing result");

                if (await CreateFailureIndexAsync(workItem).AnyContext()) {
                    foreach (var failure in lastReindexResponse.Failures) {
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

        private async Task<bool> CreateFailureIndexAsync(ReindexWorkItem workItem) {
            string errorIndex = workItem.NewIndex + "-error";
            var existsResponse = await _client.IndexExistsAsync(errorIndex).AnyContext();
            if (!existsResponse.IsValid || existsResponse.Exists)
                return true;

            var createResponse = await _client.CreateIndexAsync(errorIndex, d => d.Mappings(m => m.Map("failures", md => md.Dynamic(false)))).AnyContext();
            if (!createResponse.IsValid) {
                _logger.LogError(createResponse.OriginalException, "Unable to create error index: {Message}", createResponse.GetErrorMessage());
                return false;
            }

            return true;
        }

        private async Task HandleFailureAsync(ReindexWorkItem workItem, BulkIndexByScrollFailure failure) {
            _logger.LogError("Error reindexing document {Index}/{Type}/{Id}: [{Status}] {Message}", failure.Index, failure.Type, failure.Id, failure.Status, failure.Cause.Reason);
            var gr = await _client.GetAsync<object>(request: new GetRequest(failure.Index, failure.Type, failure.Id)).AnyContext();
            if (!gr.IsValid) {
                _logger.LogError("Error getting document {Index}/{Type}/{Id}: {Message}", failure.Index, failure.Type, failure.Id, gr.GetErrorMessage());
                return;
            }

            var document = JObject.FromObject(new {
                failure.Index,
                failure.Type,
                failure.Id,
                gr.Version,
                gr.Parent,
                gr.Source,
                failure.Cause,
                failure.Status,
                gr.Found,
            });

            var indexResponse = await _client.IndexAsync(document, d => d.Index(workItem.NewIndex + "-error").Type("failures")).AnyContext();
            if (!indexResponse.IsValid)
                _logger.LogError("Error indexing document {Index}/{Type}/{Id}: {Message}", workItem.NewIndex + "-error", gr.Type, gr.Id, indexResponse.GetErrorMessage());
        }

        private async Task<List<string>> GetIndexAliasesAsync(string index) {
            var aliasesResponse = await _client.GetAliasAsync(a => a.Index(index)).AnyContext();
            if (_logger.IsEnabled(Microsoft.Extensions.Logging.LogLevel.Trace))
                _logger.LogTrace(aliasesResponse.GetRequest());

            if (aliasesResponse.IsValid && aliasesResponse.Indices.Count > 0) {
                var aliases = aliasesResponse.Indices.Single(a => a.Key == index);
                return aliases.Value.Select(a => a.Name).ToList();
            }

            return new List<string>();
        }

        private async Task<QueryContainer> GetResumeQueryAsync(string newIndex, string timestampField, DateTime? startTime) {
            var descriptor = new QueryContainerDescriptor<object>();
            if (startTime.HasValue)
                return CreateRangeQuery(descriptor, timestampField, startTime);

            object startingPoint = await GetResumeStartingPointAsync(newIndex, timestampField ?? ID_FIELD).AnyContext();
            if (startingPoint is DateTime)
                return CreateRangeQuery(descriptor, timestampField, (DateTime)startingPoint);

            if (startingPoint is string)
                return descriptor.TermRange(dr => dr.Field(timestampField ?? ID_FIELD).GreaterThanOrEquals((string)startingPoint));

            if (startingPoint != null)
                throw new ApplicationException("Unable to create resume query from returned starting point");

            return descriptor;
        }

        private QueryContainer CreateRangeQuery(QueryContainerDescriptor<object> descriptor, string timestampField, DateTime? startTime) {
            if (!String.IsNullOrEmpty(timestampField))
                return descriptor.DateRange(dr => dr.Field(timestampField).GreaterThanOrEquals(startTime));

            return descriptor.TermRange(dr => dr.Field(ID_FIELD).GreaterThanOrEquals(ObjectId.GenerateNewId(startTime.Value).ToString()));
        }

        private async Task<object> GetResumeStartingPointAsync(string newIndex, string timestampField) {
            var newestDocumentResponse = await _client.SearchAsync<JObject>(d => d
                .Index(Indices.Index(newIndex))
                .AllTypes()
                .Sort(s => s.Descending(new Field(timestampField)))
                .Source(s => s.Includes(f => f.Field(timestampField)))
                .Size(1)
            ).AnyContext();

            if (_logger.IsEnabled(Microsoft.Extensions.Logging.LogLevel.Trace))
                _logger.LogTrace(newestDocumentResponse.GetRequest());

            if (!newestDocumentResponse.IsValid || !newestDocumentResponse.Documents.Any())
                return null;

            var token = newestDocumentResponse.Documents.FirstOrDefault()?[timestampField];
            if (token == null)
                return null;

            if (token.Type == JTokenType.Date)
                return token.ToObject<DateTime>();

            return token.ToString();
        }

        private int CalculateProgress(long total, long completed, int startProgress = 0, int endProgress = 100) {
            return startProgress + (int)((100 * (double)completed / total) * (((double)endProgress - startProgress) / 100));
        }

        private class ReindexResult {
            public long Total { get; set; }
            public long Completed { get; set; }
            public long Failures { get; set; }
            public bool Succeeded { get; set; }
        }

        private class TaskWithReindexResponse {
            public TaskReindexResult Response { get; set; }
            public TaskReindexError Error { get; set; }
        }

        private class TaskReindexError {
            public string Type { get; set; }
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
    }
}