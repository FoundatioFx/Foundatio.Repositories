using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Elasticsearch.Net.ConnectionPool;
using Foundatio.Caching;
using Foundatio.Jobs;
using Foundatio.Lock;
using Foundatio.Queues;
using Foundatio.Repositories.Elasticsearch.Jobs;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Configuration {
    public interface IDatabase {
        ICollection<IIndex> Indexes { get; }
        void ConfigureIndexes(IEnumerable<IIndex> indexes = null, bool beginReindexingOutdated = true);
        void DeleteIndexes(IEnumerable<IIndex> indexes = null);
        void Reindex(IEnumerable<IIndex> indexes = null);
        int GetIndexVersion(IIndex index);
    }

    public class Database: IDatabase {
        protected readonly ILockProvider _lockProvider = null;
        protected readonly IQueue<WorkItemData> _workItemQueue;

        public Database(Uri serverUri, IQueue<WorkItemData> workItemQueue = null, ICacheClient cacheClient = null)
            : this(new ElasticClient(new ConnectionSettings(new StaticConnectionPool(new[] { serverUri }))), workItemQueue, cacheClient) {
        }

        public Database(IEnumerable<Uri> serverUris, IQueue<WorkItemData> workItemQueue = null, ICacheClient cacheClient = null)
            : this(new ElasticClient(new ConnectionSettings(new StaticConnectionPool(serverUris))), workItemQueue, cacheClient) {
        }

        public Database(IElasticClient client, IQueue<WorkItemData> workItemQueue = null, ICacheClient cacheClient = null) {
            Client = client;
            _workItemQueue = workItemQueue;
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

        public void Reindex(IIndex index) {
            Reindex(new[] { index });
        }

        public void Reindex(IEnumerable<IIndex> indexes = null) {
            throw new NotImplementedException();
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
    }
}
