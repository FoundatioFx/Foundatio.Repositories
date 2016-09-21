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
            const int pageSize = 100;
            const string scroll = "1h";
            string timestampField = workItem.TimestampField ?? "_timestamp";
            var scopedCacheClient = new ScopedCacheClient(_cache, workItem.GetHashCode().ToString());
            
            string scrollId = await scopedCacheClient.GetAsync<string>("id", null).AnyContext();
            if (String.IsNullOrEmpty(scrollId)) {
                var scanResults = await _client.SearchAsync<JObject>(s => s
                    .Index(workItem.OldIndex)
                    .AllTypes()
                    .Query(q => q.Filtered(f => {
                        if (startTime.HasValue)
                            f.Filter(f1 => f1.Range(r => r.OnField(timestampField).Greater(startTime.Value)));
                    }))
                    .From(0).Take(pageSize)
                    .SearchType(SearchType.Scan)
                    .Scroll(scroll)).AnyContext();

                if (!scanResults.IsValid || scanResults.ScrollId == null) {
                    _logger.Error().Exception(scanResults.ConnectionStatus.OriginalException).Message("Invalid search result: message={0}", scanResults.GetErrorMessage()).Write();
                    return new ReindexResult();
                }

                scrollId = scanResults.ScrollId;
                await scopedCacheClient.AddAsync("id", scrollId, TimeSpan.FromHours(1)).AnyContext();
            } else {
                await scopedCacheClient.SetExpirationAsync("id", TimeSpan.FromHours(1)).AnyContext();
            }

            var parentMap = workItem.ParentMaps?.ToDictionary(p => p.Type, p => p.ParentPath) ?? new Dictionary<string, string>();
            var results = await _client.ScrollAsync<JObject>(scroll, scrollId).AnyContext();
            if (!results.IsValid) {
                await scopedCacheClient.RemoveAsync("id").AnyContext();
                return await InternalReindexAsync(workItem, progressCallbackAsync, startProgress, endProgress, startTime).AnyContext();
            }

            double completed = await scopedCacheClient.GetAsync<double>("completed", 0).AnyContext();
            long totalHits = results.Total;
            while (results.Documents.Any()) {
                var bulkDescriptor = new BulkDescriptor();
                foreach (var hit in results.Hits) {
                    var h = hit;
                    // TODO: Add support for doing JObject based schema migrations
                    bulkDescriptor.Index<JObject>(idx => {
                        idx
                            .Index(workItem.NewIndex)
                            .Type(h.Type)
                            .Id(h.Id)
                            .Version(h.Version)
                            .Document(h.Source);

                        if (String.IsNullOrEmpty(h.Type))
                            _logger.Error("Hit type empty. id={0}", h.Id);

                        if (parentMap.ContainsKey(h.Type)) {
                            if (String.IsNullOrEmpty(parentMap[h.Type]))
                                _logger.Error("Parent map has empty value. id={0} type={1}", h.Id, h.Type);

                            var parentId = h.Source.SelectToken(parentMap[h.Type]);
                            if (!String.IsNullOrEmpty(parentId?.ToString()))
                                idx.Parent(parentId.ToString());
                            else
                                _logger.Error("Unable to get parent id. id={0} path={1}", h.Id, parentMap[h.Type]);
                        }

                        return idx;
                    });
                }

                var bulkResponse = await _client.BulkAsync(bulkDescriptor).AnyContext();
                if (!bulkResponse.IsValid) {
                    string message = $"Reindex bulk error: old={workItem.OldIndex} new={workItem.NewIndex} completed={completed} message={bulkResponse.GetErrorMessage()}";
                    _logger.Warn(bulkResponse.ConnectionStatus.OriginalException, message);
                    // try each doc individually so we can see which doc is breaking us
                    foreach (var itemWithError in bulkResponse.ItemsWithErrors) {
                        var h = results.Hits.First(hit => hit.Id == itemWithError.Id);
                        var response = await _client.IndexAsync<JObject>(h.Source, d => {
                            long version;
                            if (!Int64.TryParse(h.Version, out version))
                                version = 1;

                            d
                                .Index(workItem.NewIndex)
                                .Type(h.Type)
                                .Version(version)
                                .Id(h.Id);

                            if (parentMap.ContainsKey(h.Type)) {
                                var parentId = h.Source.SelectToken(parentMap[h.Type]);
                                if (!String.IsNullOrEmpty(parentId?.ToString()))
                                    d.Parent(parentId.ToString());
                                else
                                    _logger.Error("Unable to get parent id. id={0} path={1}", h.Id, parentMap[h.Type]);
                            }

                            return d;
                        }).AnyContext();

                        if (response.IsValid)
                            continue;

                        message = $"Reindex error: old={workItem.OldIndex} new={workItem.NewIndex} id={itemWithError.Id} completed={completed} message={response.GetErrorMessage()}";
                        _logger.Error().Exception(response.ConnectionStatus.OriginalException).Message(message);

                        var errorDoc = new JObject(new {
                            h.Type,
                            Content = h.Source.ToString(Formatting.Indented)
                        });

                        if (parentMap.ContainsKey(h.Type)) {
                            var parentId = h.Source.SelectToken(parentMap[h.Type]);
                            if (!String.IsNullOrEmpty(parentId?.ToString()))
                                errorDoc["ParentId"] = parentId.ToString();
                            else
                                _logger.Error("Unable to get parent id. id={0} path={1}", h.Id, parentMap[h.Type]);
                        }

                        // put the document into an error index
                        response = await _client.IndexAsync<JObject>(errorDoc, d => {
                            d.Index(workItem.NewIndex + "-error")
                             .Id(h.Id);
                            return d;
                        }).AnyContext();

                        if (response.IsValid)
                            continue;

                        message = $"Reindex error: old={workItem.OldIndex} new={workItem.NewIndex} id={itemWithError.Id} completed={completed} message={response.GetErrorMessage()}";
                        _logger.Error().Exception(response.ConnectionStatus.OriginalException).Message(message);
                        throw new ReindexException(response.ConnectionStatus, message);
                    }
                }
                
                completed = await scopedCacheClient.IncrementAsync("completed", bulkResponse.Items.Count(), TimeSpan.FromHours(1)).AnyContext();
                await progressCallbackAsync(CalculateProgress(totalHits, (long)completed, startProgress, endProgress), $"Total: {totalHits} Completed: {completed}").AnyContext();
                results = await _client.ScrollAsync<JObject>(scroll, results.ScrollId).AnyContext();
                await scopedCacheClient.AddAsync("id", results.ScrollId, TimeSpan.FromHours(1)).AnyContext();
            }

            await scopedCacheClient.RemoveAllAsync(new []{ "id", "completed" }).AnyContext();
            return new ReindexResult { Total = totalHits, Completed = (long)completed };
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