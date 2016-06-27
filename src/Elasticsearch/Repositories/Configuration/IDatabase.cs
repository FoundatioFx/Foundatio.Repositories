using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Elasticsearch.Net;
using Elasticsearch.Net.ConnectionPool;
using Foundatio.Caching;
using Foundatio.Jobs;
using Foundatio.Lock;
using Foundatio.Logging;
using Foundatio.Queues;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Jobs;
using Foundatio.Repositories.Extensions;
using Nest;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;

namespace Foundatio.Repositories.Elasticsearch.Configuration {
    public interface IDatabase {
        ICollection<IIndex> Indexes { get; }
        void ConfigureIndexes(IEnumerable<IIndex> indexes = null, bool beginReindexingOutdated = true);
        void DeleteIndexes(IEnumerable<IIndex> indexes = null);
        int GetIndexVersion(IIndex index);
        Task ReindexAsync(IEnumerable<IIndex> indexes = null, Func<int, string, Task> progressCallbackAsync = null);
        Task ReindexAsync(ReindexWorkItem workItem, Func<int, string, Task> progressCallbackAsync = null);
    }

    public class Database: IDatabase {
        protected readonly ILockProvider _lockProvider = null;
        protected readonly IQueue<WorkItemData> _workItemQueue;
        protected readonly ILogger _logger;

        public Database(Uri serverUri, IQueue<WorkItemData> workItemQueue = null, ICacheClient cacheClient = null, ILogger logger = null)
            : this(new ElasticClient(new ConnectionSettings(new StaticConnectionPool(new[] { serverUri }))), workItemQueue, cacheClient) {
        }

        public Database(IEnumerable<Uri> serverUris, IQueue<WorkItemData> workItemQueue = null, ICacheClient cacheClient = null, ILogger logger = null)
            : this(new ElasticClient(new ConnectionSettings(new StaticConnectionPool(serverUris))), workItemQueue, cacheClient) {
        }

        public Database(IElasticClient client, IQueue<WorkItemData> workItemQueue = null, ICacheClient cacheClient = null, ILogger logger = null) {
            Client = client;
            _workItemQueue = workItemQueue;
            _logger = logger;
            if (cacheClient != null)
                _lockProvider = new ThrottlingLockProvider(cacheClient, 1, TimeSpan.FromMinutes(1));
        }

        public IElasticClient Client { get; }

        public ICollection<IIndex> Indexes { get; } = new List<IIndex>();

        public void ConfigureIndex(IIndex index, bool beginReindexingOutdated = true) {
            ConfigureIndexes(new[] { index }, beginReindexingOutdated);
        }

        public void ConfigureIndexes(IEnumerable<IIndex> indexes = null, bool beginReindexingOutdated = true) {
            if (indexes == null)
                indexes = Indexes;

            foreach (var idx in indexes) {
                IIndicesOperationResponse response = null;
                var templatedIndex = idx as ITimeSeriesIndex;
                if (templatedIndex != null)
                    response = Client.PutTemplate(idx.VersionedName, template => templatedIndex.ConfigureTemplate(template));
                else if (!Client.IndexExists(idx.VersionedName).Exists)
                    response = Client.CreateIndex(idx.VersionedName, descriptor => idx.ConfigureIndex(descriptor));

                Debug.Assert(response == null || response.IsValid, response?.ServerError != null ? response.ServerError.Error : "An error occurred creating the index or template.");

                // Add existing indexes to the alias.
                if (!Client.AliasExists(idx.AliasName).Exists) {
                    if (templatedIndex != null) {
                        var indices = Client.IndicesStats().Indices.Where(kvp => kvp.Key.StartsWith(idx.VersionedName)).Select(kvp => kvp.Key).ToList();
                        if (indices.Count > 0) {
                            var descriptor = new AliasDescriptor();
                            foreach (string name in indices)
                                descriptor.Add(add => add.Index(name).Alias(idx.AliasName));

                            response = Client.Alias(descriptor);
                        }
                    } else {
                        response = Client.Alias(a => a.Add(add => add.Index(idx.VersionedName).Alias(idx.AliasName)));
                    }

                    Debug.Assert(response != null && response.IsValid, response?.ServerError != null ? response.ServerError.Error : "An error occurred creating the alias.");
                }

                if (!beginReindexingOutdated)
                    continue;

                if (_workItemQueue == null || _lockProvider == null)
                    throw new InvalidOperationException("Must specify work item queue and lock provider in order to reindex.");

                int currentVersion = GetIndexVersion(idx);

                // already on current version
                if (currentVersion >= idx.Version || currentVersion < 1)
                    continue;

                var reindexWorkItem = new ReindexWorkItem {
                    OldIndex = String.Concat(idx.AliasName, "-v", currentVersion),
                    NewIndex = idx.VersionedName,
                    Alias = idx.AliasName,
                    DeleteOld = true
                };

                foreach (var type in idx.IndexTypes.OfType<IChildIndexType>())
                    reindexWorkItem.ParentMaps.Add(new ParentMap { Type = type.Name, ParentPath = type.ParentPath });

                bool isReindexing = _lockProvider.IsLockedAsync(String.Concat("reindex:", reindexWorkItem.Alias, reindexWorkItem.OldIndex, reindexWorkItem.NewIndex)).Result;
                // already reindexing
                if (isReindexing)
                    continue;

                // enqueue reindex to new version
                _lockProvider.TryUsingAsync("enqueue-reindex", () => _workItemQueue.EnqueueAsync(reindexWorkItem), TimeSpan.Zero, CancellationToken.None).Wait();
            }
        }

        public void DeleteIndex(IIndex index) {
            DeleteIndexes(new[] { index });
        }

        public void DeleteIndexes(IEnumerable<IIndex> indexes = null) {
            if (indexes == null)
                indexes = Indexes;

            foreach (var idx in indexes) {
                IIndicesResponse deleteResponse;

                var templatedIndex = idx as ITimeSeriesIndex;
                if (templatedIndex != null) {
                    deleteResponse = Client.DeleteIndex(idx.VersionedName + "-*");

                    if (Client.TemplateExists(idx.VersionedName).Exists) {
                        var response = Client.DeleteTemplate(idx.VersionedName);
                        Debug.Assert(response.IsValid, response.ServerError != null ? response.ServerError.Error : "An error occurred deleting the index template.");
                    }
                } else {
                    deleteResponse = Client.DeleteIndex(idx.VersionedName);
                }

                Debug.Assert(deleteResponse.IsValid, deleteResponse.ServerError != null ? deleteResponse.ServerError.Error : "An error occurred deleting the indexes.");
            }
        }

        public int GetIndexVersion(IIndex index) {
            var res = Client.GetAlias(a => a.Alias(index.AliasName));
            if (!res.Indices.Any())
                return -1;

            string indexName = res.Indices.FirstOrDefault().Key;
            string versionString = indexName.Substring(indexName.LastIndexOf("-", StringComparison.Ordinal));

            int version;
            if (!Int32.TryParse(versionString.Substring(2), out version))
                return -1;

            return version;
        }

        public async Task ReindexAsync(ReindexWorkItem workItem, Func<int, string, Task> progressCallbackAsync = null) {
            if (progressCallbackAsync == null)
                progressCallbackAsync = (i, s) => Task.CompletedTask;

            long existingDocCount = (await Client.CountAsync(d => d.Index(workItem.NewIndex)).AnyContext()).Count;
            _logger.Info("Received reindex work item for new index {0}", workItem.NewIndex);
            var startTime = DateTime.UtcNow.AddSeconds(-1);
            await progressCallbackAsync(0, "Starting reindex...").AnyContext();
            var result = await InternalReindexAsync(workItem, progressCallbackAsync, 0, 90, workItem.StartUtc).AnyContext();
            await progressCallbackAsync(90, $"Total: {result.Total} Completed: {result.Completed}").AnyContext();

            // TODO: Check to make sure the docs have been added to the new index before changing alias

            if (!String.IsNullOrEmpty(workItem.Alias)) {
                await Client.AliasAsync(x => x
                    .Remove(a => a.Alias(workItem.Alias).Index(workItem.OldIndex))
                    .Add(a => a.Alias(workItem.Alias).Index(workItem.NewIndex))).AnyContext();

                await progressCallbackAsync(98, $"Updated alias: {workItem.Alias} Remove: {workItem.OldIndex} Add: {workItem.NewIndex}").AnyContext();
            }

            await Client.RefreshAsync().AnyContext();
            var secondPassResult = await InternalReindexAsync(workItem, progressCallbackAsync, 90, 98, startTime).AnyContext();
            await progressCallbackAsync(98, $"Total: {secondPassResult.Total} Completed: {secondPassResult.Completed}").AnyContext();

            if (workItem.DeleteOld) {
                await Client.RefreshAsync().AnyContext();
                long newDocCount = (await Client.CountAsync(d => d.Index(workItem.NewIndex)).AnyContext()).Count - existingDocCount;
                long oldDocCount = (await Client.CountAsync(d => d.Index(workItem.OldIndex)).AnyContext()).Count;
                await progressCallbackAsync(98, $"Old Docs: {oldDocCount} New Docs: {newDocCount}").AnyContext();
                if (newDocCount >= oldDocCount)
                    await Client.DeleteIndexAsync(d => d.Index(workItem.OldIndex)).AnyContext();
                await progressCallbackAsync(98, $"Deleted index: {workItem.OldIndex}").AnyContext();
            }
            await progressCallbackAsync(100, null).AnyContext();
        }

        private async Task<ReindexResult> InternalReindexAsync(ReindexWorkItem workItem, Func<int, string, Task> progressCallbackAsync, int startProgress = 0, int endProgress = 100, DateTime? startTime = null) {
            const int pageSize = 100;
            const string scroll = "5m";
            string timestampField = workItem.TimestampField ?? "_timestamp";

            long completed = 0;

            var scanResults = await Client.SearchAsync<JObject>(s => s
                .Index(workItem.OldIndex)
                .AllTypes()
                .Filter(f => startTime.HasValue
                    ? f.Range(r => r.OnField(timestampField).Greater(startTime.Value))
                    : f.MatchAll())
                .From(0).Take(pageSize)
                .SearchType(SearchType.Scan)
                .Scroll(scroll)).AnyContext();

            if (!scanResults.IsValid || scanResults.ScrollId == null) {
                _logger.Error().Message("Invalid search result: message={0}", scanResults.GetErrorMessage()).Write();
                return new ReindexResult();
            }

            long totalHits = scanResults.Total;

            var parentMap = workItem.ParentMaps?.ToDictionary(p => p.Type, p => p.ParentPath) ?? new Dictionary<string, string>();

            var results = await Client.ScrollAsync<JObject>(scroll, scanResults.ScrollId).AnyContext();
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

                var bulkResponse = await Client.BulkAsync(bulkDescriptor).AnyContext();
                if (!bulkResponse.IsValid) {
                    string message = $"Reindex bulk error: old={workItem.OldIndex} new={workItem.NewIndex} completed={completed} message={bulkResponse.GetErrorMessage()}";
                    _logger.Warn(message);
                    // try each doc individually so we can see which doc is breaking us
                    foreach (var hit in results.Hits) {
                        var h = hit;
                        var response = await Client.IndexAsync<JObject>(h.Source, d => {
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

                        message = $"Reindex error: old={workItem.OldIndex} new={workItem.NewIndex} id={hit.Id} completed={completed} message={response.GetErrorMessage()}";
                        _logger.Error(message);

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
                        response = await Client.IndexAsync<JObject>(errorDoc, d => {
                            d
                                .Index(workItem.NewIndex + "-error")
                                .Id(h.Id);

                            return d;
                        }).AnyContext();

                        if (response.IsValid)
                            continue;

                        throw new ReindexException(response.ConnectionStatus, message);
                    }
                }

                completed += bulkResponse.Items.Count();
                await progressCallbackAsync(CalculateProgress(totalHits, completed, startProgress, endProgress),
                    $"Total: {totalHits} Completed: {completed}").AnyContext();

                _logger.Info().Message($"Reindex Progress: {CalculateProgress(totalHits, completed, startProgress, endProgress)} Completed: {completed} Total: {totalHits}").Write();
                results = await Client.ScrollAsync<JObject>(scroll, results.ScrollId).AnyContext();
            }

            return new ReindexResult { Total = totalHits, Completed = completed };
        }

        protected int CalculateProgress(long total, long completed, int startProgress = 0, int endProgress = 100) {
            return startProgress + (int)((100 * (double)completed / total) * (((double)endProgress - startProgress) / 100));
        }

        private class ReindexResult {
            public long Total { get; set; }
            public long Completed { get; set; }
        }

        public Task ReindexAsync(IIndex index, Func<int, string, Task> progressCallbackAsync = null) {
            return ReindexAsync(new[] { index }, progressCallbackAsync);
        }

        public async Task ReindexAsync(IEnumerable<IIndex> indexes = null, Func<int, string, Task> progressCallbackAsync = null) {
        }
    }
}
