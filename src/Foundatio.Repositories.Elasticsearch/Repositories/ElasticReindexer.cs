using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Elasticsearch.Net;
using Foundatio.Logging;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Jobs;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Utility;
using Foundatio.Utility;
using Nest;
using Newtonsoft.Json.Linq;

namespace Foundatio.Repositories.Elasticsearch {
    public class ElasticReindexer {
        private readonly IElasticClient _client;
        private readonly ILogger _logger;
        private const string ID_FIELD = "id";

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
                    _logger.Info("Reindex Progress {0}%: {1}", progress, message);
                    return Task.CompletedTask;
                };
            }

            _logger.Info("Received reindex work item for new index {0}", workItem.NewIndex);
            var startTime = SystemClock.UtcNow.AddSeconds(-1);
            await progressCallbackAsync(0, "Starting reindex...").AnyContext();
            var firstPassResult = await InternalReindexAsync(workItem, progressCallbackAsync, 0, 90, workItem.StartUtc).AnyContext();
            await progressCallbackAsync(91, $"Total: {firstPassResult.Total} Completed: {firstPassResult.Completed}").AnyContext();

            // TODO: Check to make sure the docs have been added to the new index before changing alias
            if (workItem.OldIndex != workItem.NewIndex) {
                var aliases = await GetIndexAliasesAsync(workItem.OldIndex).AnyContext();
                if (!String.IsNullOrEmpty(workItem.Alias) && !aliases.Contains(workItem.Alias))
                    aliases.Add(workItem.Alias);

                if (aliases.Count > 0) {
                    var bulkResponse = await _client.AliasAsync(x => {
                        foreach (var alias in aliases)
                            x = x.Remove(a => a.Alias(alias).Index(workItem.OldIndex)).Add(a => a.Alias(alias).Index(workItem.NewIndex));

                        return x;
                    }).AnyContext();
                    _logger.Trace(() => bulkResponse.GetRequest());

                    await progressCallbackAsync(92, $"Updated aliases: {String.Join(", ", aliases)} Remove: {workItem.OldIndex} Add: {workItem.NewIndex}").AnyContext();
                }
            }

            await _client.RefreshAsync(Indices.All).AnyContext();
            var secondPassResult = await InternalReindexAsync(workItem, progressCallbackAsync, 92, 96, startTime).AnyContext();
            await progressCallbackAsync(97, $"Total: {secondPassResult.Total} Completed: {secondPassResult.Completed}").AnyContext();

            bool hasFailures = (firstPassResult.Failures + secondPassResult.Failures) > 0;
            if (!hasFailures && workItem.DeleteOld && workItem.OldIndex != workItem.NewIndex) {
                await _client.RefreshAsync(Indices.All).AnyContext();
                long newDocCount = (await _client.CountAsync<object>(d => d.Index(workItem.NewIndex)).AnyContext()).Count;
                long oldDocCount = (await _client.CountAsync<object>(d => d.Index(workItem.OldIndex)).AnyContext()).Count;
                await progressCallbackAsync(98, $"Old Docs: {oldDocCount} New Docs: {newDocCount}").AnyContext();
                if (newDocCount >= oldDocCount) {
                    await _client.DeleteIndexAsync(Indices.Index(workItem.OldIndex)).AnyContext();
                    await progressCallbackAsync(99, $"Deleted index: {workItem.OldIndex}").AnyContext();
                }
            }

            await progressCallbackAsync(100, null).AnyContext();
        }

        private async Task<ReindexResult> InternalReindexAsync(ReindexWorkItem workItem, Func<int, string, Task> progressCallbackAsync, int startProgress = 0, int endProgress = 100, DateTime? startTime = null) {
            var query = await GetResumeQueryAsync(workItem.NewIndex, workItem.TimestampField, startTime).AnyContext();
            var response = await _client.ReindexOnServerAsync(d => d
                .Source(src => src
                    .Index(workItem.OldIndex)
                    .Query<object>(q => query)
                    .Sort<object>(s => s.Ascending(new Field(workItem.TimestampField ?? ID_FIELD))))
                .Destination(dest => dest.Index(workItem.NewIndex))
                .Conflicts(Conflicts.Proceed)).AnyContext();

            _logger.Trace(() => response.GetRequest());

            long failures = 0;
            if (!response.IsValid) {
                _logger.Error().Exception(response.OriginalException).Message("Error while reindexing result: {0}", response.GetErrorMessage()).Write();

                if (await CreateFailureIndexAsync(workItem).AnyContext()) {
                    foreach (var failure in response.Failures) {
                        await HandleFailureAsync(workItem, failure).AnyContext();
                        failures++;
                    }
                }
            }

            long completed = response.Created + response.Updated + response.Noops;
            string message = $"Total: {response.Total} Completed: {completed} VersionConflicts: {response.VersionConflicts}";
            await progressCallbackAsync(CalculateProgress(response.Total, completed, startProgress, endProgress), message).AnyContext();
            return new ReindexResult { Total = response.Total, Completed = completed, Failures = failures };
        }

        private async Task<bool> CreateFailureIndexAsync(ReindexWorkItem workItem) {
            string errorIndex = workItem.NewIndex + "-error";
            var existsResponse = await _client.IndexExistsAsync(errorIndex).AnyContext();
            if (!existsResponse.IsValid || existsResponse.Exists)
                return true;

            var createResponse = await _client.CreateIndexAsync(errorIndex, d => d.Mappings(m => m.Map("failures", md => md.Dynamic(false)))).AnyContext();
            if (!createResponse.IsValid) {
                _logger.Error().Exception(createResponse.OriginalException).Message("Unable to create error index: {0}", createResponse.GetErrorMessage()).Write();
                return false;
            }

            return true;
        }

        private async Task HandleFailureAsync(ReindexWorkItem workItem, BulkIndexByScrollFailure failure) {
            _logger.Error().Message("Error reindexing document {0}/{1}/{2}: [{3}] {4}", failure.Index, failure.Type, failure.Id, failure.Status, failure.Cause.Reason).Write();
            var gr = await _client.GetAsync<object>(request: new GetRequest(failure.Index, failure.Type, failure.Id)).AnyContext();
            if (!gr.IsValid) {
                _logger.Error().Message("Error getting document {0}/{1}/{2}: {3}", failure.Index, failure.Type, failure.Id, gr.GetErrorMessage()).Write();
                return;
            }

            var document = new JObject(new {
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
                _logger.Error().Message("Error indexing document {0}/{1}/{2}: {3}", workItem.NewIndex + "-error", gr.Type, gr.Id, indexResponse.GetErrorMessage()).Write();
        }

        private async Task<List<string>> GetIndexAliasesAsync(string index) {
            var aliasesResponse = await _client.GetAliasAsync(a => a.Index(index)).AnyContext();
            _logger.Trace(() => aliasesResponse.GetRequest());

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

            var startingPoint = await GetResumeStartingPointAsync(newIndex, timestampField ?? ID_FIELD).AnyContext();
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

            _logger.Trace(() => newestDocumentResponse.GetRequest());
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
        }
    }
}