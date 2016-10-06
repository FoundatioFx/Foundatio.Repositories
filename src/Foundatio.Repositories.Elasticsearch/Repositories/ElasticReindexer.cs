using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Elasticsearch.Net;
using Foundatio.Logging;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Jobs;
using Foundatio.Repositories.Extensions;
using Foundatio.Utility;
using Nest;
using Newtonsoft.Json.Linq;

namespace Foundatio.Repositories.Elasticsearch {
    public class ElasticReindexer {
        private readonly IElasticClient _client;
        private readonly ILogger _logger;

        public ElasticReindexer(IElasticClient client, ILogger logger = null) {
            _client = client;
            _logger = logger ?? NullLogger.Instance;
        }

        public async Task ReindexAsync(ReindexWorkItem workItem, Func<int, string, Task> progressCallbackAsync = null) {
            if (String.IsNullOrEmpty(workItem?.TimestampField))
                throw new ArgumentNullException(nameof(workItem.TimestampField));

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
            var result = await InternalReindexAsync(workItem, progressCallbackAsync, 0, 90, workItem.StartUtc).AnyContext();
            await progressCallbackAsync(91, $"Total: {result.Total} Completed: {result.Completed}").AnyContext();

            // TODO: Check to make sure the docs have been added to the new index before changing alias
            if (workItem.OldIndex != workItem.NewIndex) {
                var aliases = await GetIndexAliases(workItem.OldIndex).AnyContext();
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

            if (workItem.DeleteOld && workItem.OldIndex != workItem.NewIndex) {
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
                    .Sort<object>(s => s.Ascending(new Field(workItem.TimestampField))))
                .Destination(dest => dest.Index(workItem.NewIndex))
                .Conflicts(Conflicts.Proceed)).AnyContext();

            _logger.Trace(() => response.GetRequest());
            if (!response.IsValid) {
                _logger.Error().Exception(response.OriginalException).Message("Error while reindexing result: {0}", response.GetErrorMessage()).Write();
                return new ReindexResult();
            }

            // TODO: Store invalid documents into a new index.
            // TODO: Second pass on object id redose everything..
            long completed = response.Created + response.Updated + response.Noops;
            string message = $"Total: {response.Total} Completed: {completed} VersionConflicts: {response.VersionConflicts}";
            await progressCallbackAsync(CalculateProgress(response.Total, completed, startProgress, endProgress), message).AnyContext();
            return new ReindexResult { Total = response.Total, Completed = completed };
        }

        private async Task<List<string>> GetIndexAliases(string index) {
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
                return descriptor.DateRange(dr => dr.Field(timestampField).GreaterThanOrEquals(startTime));

            var startingPoint = await GetResumeStartingPointAsync(newIndex, timestampField).AnyContext();
            if (startingPoint is DateTime)
                return descriptor.DateRange(dr => dr.Field(timestampField).GreaterThanOrEquals((DateTime)startingPoint));
            if (startingPoint is string) // TODO: Change this to use a range query: https://github.com/elastic/elasticsearch-net/issues/2307
                return descriptor.QueryString(qs => qs.DefaultField(timestampField).Query($">={(string)startingPoint}"));

            return descriptor;
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
        }
    }
}