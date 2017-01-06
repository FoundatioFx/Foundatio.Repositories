using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Elasticsearch.Net;
using Foundatio.Caching;
using Foundatio.Logging;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Jobs;
using Foundatio.Repositories.Extensions;
using Foundatio.Utility;
using Nest;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Foundatio.Repositories.Elasticsearch {
    public class ElasticReindexer {
        private readonly IElasticClient _client;
        private readonly ICacheClient _cache;
        private readonly ILogger _logger;

        public ElasticReindexer(IElasticClient client, ICacheClient cache = null, ILogger logger = null) {
            _client = client;
            _cache = new ScopedCacheClient(cache ?? new NullCacheClient(), "reindex");
            _logger = logger ?? NullLogger.Instance;
        }

        public async Task ReindexAsync(ReindexWorkItem workItem, Func<int, string, Task> progressCallbackAsync = null) {
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
            await progressCallbackAsync(95, $"Total: {result.Total} Completed: {result.Completed}").AnyContext();

            // TODO: Check to make sure the docs have been added to the new index before changing alias
            if (workItem.OldIndex != workItem.NewIndex) {
                var aliases = await GetIndexAliases(workItem.OldIndex).AnyContext();
                if (!String.IsNullOrEmpty(workItem.Alias) && !aliases.Contains(workItem.Alias))
                    aliases.Add(workItem.Alias);

                if (aliases.Count > 0) {
                    await _client.AliasAsync(x => {
                        foreach (var alias in aliases)
                            x = x.Remove(a => a.Alias(alias).Index(workItem.OldIndex)).Add(a => a.Alias(alias).Index(workItem.NewIndex));

                        return x;
                    }).AnyContext();

                    await progressCallbackAsync(98, $"Updated aliases: {String.Join(", ", aliases)} Remove: {workItem.OldIndex} Add: {workItem.NewIndex}").AnyContext();
                }
            }

            await _client.RefreshAsync().AnyContext();
            var secondPassResult = await InternalReindexAsync(workItem, progressCallbackAsync, 90, 98, startTime).AnyContext();
            await progressCallbackAsync(98, $"Total: {secondPassResult.Total} Completed: {secondPassResult.Completed}").AnyContext();

            if (workItem.DeleteOld && workItem.OldIndex != workItem.NewIndex) {
                await _client.RefreshAsync().AnyContext();
                long newDocCount = (await _client.CountAsync(d => d.Index(workItem.NewIndex)).AnyContext()).Count;
                long oldDocCount = (await _client.CountAsync(d => d.Index(workItem.OldIndex)).AnyContext()).Count;
                await progressCallbackAsync(98, $"Old Docs: {oldDocCount} New Docs: {newDocCount}").AnyContext();
                if (newDocCount >= oldDocCount) {
                    await _client.DeleteIndexAsync(d => d.Index(workItem.OldIndex)).AnyContext();
                    await progressCallbackAsync(99, $"Deleted index: {workItem.OldIndex}").AnyContext();
                }
            }

            await progressCallbackAsync(100, null).AnyContext();
        }

        private async Task<List<string>> GetIndexAliases(string index) {
            var aliasesResponse = await _client.GetAliasesAsync(a => a.Index(index)).AnyContext();
            if (aliasesResponse.IsValid && aliasesResponse.Indices.Count > 0) {
                var aliases = aliasesResponse.Indices.Single(a => a.Key == index);
                return aliases.Value.Select(a => a.Name).ToList();
            }

            return new List<string>();
        }

        private async Task<ReindexResult> InternalReindexAsync(ReindexWorkItem workItem, Func<int, string, Task> progressCallbackAsync, int startProgress = 0, int endProgress = 100, DateTime? startTime = null) {
            const string scroll = "5m";
            bool errorIndexCreated = false;
            string timestampField = workItem.TimestampField ?? "_timestamp";
            var scopedCacheClient = new ScopedCacheClient(_cache, workItem.GetHashCode().ToString());

            var settingsResponse = await _client.GetIndexSettingsAsync(s => s.Index(workItem.OldIndex)).AnyContext();
            if (!settingsResponse.IsValid)
                throw new ApplicationException("Unable to retrieve index settings.");

            int scrollSize = 500 / settingsResponse.IndexSettings.NumberOfShards ?? 50;

            var scanResults = await _client.SearchAsync<object>(s => s
                .Index(workItem.OldIndex)
                .AllTypes()
                .Query(q => q.Filtered(f => {
                    if (startTime.HasValue)
                        f.Filter(f1 => f1.Range(r => r.OnField(timestampField).Greater(startTime.Value)));
                }))
                .Fields("_source", "_parent")
                .Size(scrollSize)
                .SearchType(SearchType.Scan)
                .Scroll(scroll)).AnyContext();

            _logger.Info(scanResults.GetRequest());

            if (!scanResults.IsValid || scanResults.ScrollId == null) {
                _logger.Error().Exception(scanResults.ConnectionStatus.OriginalException).Message("Invalid search result: message={0}", scanResults.GetErrorMessage()).Write();
                return new ReindexResult();
            }

            var results = await _client.ScrollAsync<JObject>("5m", scanResults.ScrollId).AnyContext();
            if (!results.IsValid) {
                await scopedCacheClient.RemoveAsync("id").AnyContext();
                return await InternalReindexAsync(workItem, progressCallbackAsync, startProgress, endProgress, startTime).AnyContext();
            }

            double completed = 0;
            long totalHits = results.Total;
            while (results.Hits.Any()) {
                ISearchResponse<JObject> results1 = results;

                IBulkResponse bulkResponse = null;
                try {
                    bulkResponse = await Run.WithRetriesAsync(() => _client.BulkAsync(b => {
                        foreach (var h in results1.Hits)
                            ConfigureIndexItem(b, h, workItem.NewIndex);

                        return b;
                    }), logger: _logger, maxAttempts: 2).AnyContext();
                } catch (Exception ex) {
                    _logger.Error(ex, $"Error trying to do bulk index: {ex.Message}");
                }

                if (bulkResponse == null || !bulkResponse.IsValid || bulkResponse.ItemsWithErrors.Any()) {
                    string message;
                    if (bulkResponse != null) {
                        message = $"Reindex bulk error: old={workItem.OldIndex} new={workItem.NewIndex} completed={completed} message={bulkResponse?.GetErrorMessage()}";
                        _logger.Warn(bulkResponse.ConnectionStatus.OriginalException, message);
                    }

                    // try each doc individually so we can see which doc is breaking us
                    var hitsToRetry = bulkResponse?.ItemsWithErrors.Select(i => results1.Hits.First(hit => hit.Id == i.Id)) ?? results1.Hits;
                    foreach (var itemToRetry in hitsToRetry) {
                        IIndexResponse response;
                        try {
                            response = await _client.IndexAsync(itemToRetry.Source, d => ConfigureItem(d, itemToRetry, workItem.NewIndex)).AnyContext();
                            if (response.IsValid)
                                continue;

                            message = $"Reindex error: old={workItem.OldIndex} new={workItem.NewIndex} id={itemToRetry.Id} completed={completed} message={response.GetErrorMessage()}";
                            _logger.Error().Exception(response.ConnectionStatus.OriginalException).Message(message);

                            var errorDoc = new JObject {
                                ["Type"] = itemToRetry.Type,
                                ["Content"] = itemToRetry.Source.ToString(Formatting.Indented)
                            };

                            var errorIndex = workItem.NewIndex + "-error";
                            if (!errorIndexCreated && !(await _client.IndexExistsAsync(errorIndex).AnyContext()).Exists) {
                                await _client.CreateIndexAsync(errorIndex).AnyContext();
                                errorIndexCreated = true;
                            }

                            // put the document into an error index
                            response = await _client.IndexAsync(errorDoc, d => d.Index(errorIndex).Id(itemToRetry.Id)).AnyContext();
                            if (response.IsValid)
                                continue;
                        } catch (Exception exception) {
                            throw;
                        }

                        message = $"Reindex error: old={workItem.OldIndex} new={workItem.NewIndex} id={itemToRetry.Id} completed={completed} message={response.GetErrorMessage()}";
                        _logger.Error().Exception(response.ConnectionStatus.OriginalException).Message(message);
                        throw new ReindexException(response.ConnectionStatus, message);
                    }
                }

                completed += results.Hits.Count();
                await progressCallbackAsync(CalculateProgress(totalHits, (long)completed, startProgress, endProgress), $"Total: {totalHits} Completed: {completed}").AnyContext();
                results = await _client.ScrollAsync<JObject>("5m", results.ScrollId).AnyContext();
                await scopedCacheClient.AddAsync("id", results.ScrollId, TimeSpan.FromHours(1)).AnyContext();
            }

            await scopedCacheClient.RemoveAllAsync(new[] { "id" }).AnyContext();
            return new ReindexResult { Total = totalHits, Completed = (long)completed };
        }

        private void ConfigureIndexItem(BulkDescriptor d, IHit<JObject> hit, string targetIndex) {
            d.Index<JObject>(idx => ConfigureItem(idx, hit, targetIndex));
        }

        private BulkIndexDescriptor<JObject> ConfigureItem(BulkIndexDescriptor<JObject> idx, IHit<JObject> hit, string targetIndex) {
            idx.Index(targetIndex);
            idx.Type(hit.Type);
            idx.Id(hit.Id);
            idx.Version(hit.Version);
            idx.Document(hit.Source);

            if (hit.Fields?.FieldValuesDictionary != null && hit.Fields.FieldValuesDictionary.ContainsKey("_parent"))
                idx.Parent(hit.Fields.FieldValuesDictionary["_parent"].ToString());

            return idx;
        }

        private IndexDescriptor<JObject> ConfigureItem(IndexDescriptor<JObject> idx, IHit<JObject> hit, string targetIndex) {
            idx.Index(targetIndex);
            idx.Type(hit.Type);
            idx.Id(hit.Id);

            if (!String.IsNullOrEmpty(hit.Version)) {
                long version;
                if (!Int64.TryParse(hit.Version, out version))
                    version = 1;

                idx.Version(version);
            }

            if (hit.Fields?.FieldValuesDictionary != null && hit.Fields.FieldValuesDictionary.ContainsKey("_parent"))
                idx.Parent(hit.Fields.FieldValuesDictionary["_parent"].ToString());

            return idx;
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