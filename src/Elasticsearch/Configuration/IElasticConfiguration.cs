using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Elasticsearch.Net.ConnectionPool;
using Foundatio.Caching;
using Foundatio.Jobs;
using Foundatio.Lock;
using Foundatio.Logging;
using Foundatio.Queues;
using Foundatio.Repositories.Extensions;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Configuration {
    public interface IElasticConfiguration {
        IElasticClient Client { get; }
        IReadOnlyCollection<IIndex> Indexes { get; }
        void ConfigureIndexes(IEnumerable<IIndex> indexes = null, bool beginReindexingOutdated = true);
        void DeleteIndexes(IEnumerable<IIndex> indexes = null);
        Task ReindexAsync(IEnumerable<IIndex> indexes = null, Func < int, string, Task> progressCallbackAsync = null);
    }

    public class ElasticConfiguration: IElasticConfiguration {
        protected readonly ILockProvider _lockProvider = null;
        protected readonly IQueue<WorkItemData> _workItemQueue;
        protected readonly ILogger _logger;
        private readonly List<IIndex> _indexes = new List<IIndex>();
        private readonly Lazy<IReadOnlyCollection<IIndex>> _frozenIndexes;
        
        public ElasticConfiguration(IElasticClient client, IQueue<WorkItemData> workItemQueue, ICacheClient cacheClient, ILoggerFactory loggerFactory) {
            Client = client;
            _workItemQueue = workItemQueue;
            _logger = loggerFactory.CreateLogger(GetType());
            if (cacheClient != null)
                _lockProvider = new ThrottlingLockProvider(cacheClient, 1, TimeSpan.FromMinutes(1));

            _frozenIndexes = new Lazy<IReadOnlyCollection<IIndex>>(() => _indexes.AsReadOnly());
        }
        
        public IElasticClient Client { get; protected set; }
        public IReadOnlyCollection<IIndex> Indexes => _frozenIndexes.Value;

        public void AddIndex(IIndex index) {
            if (_frozenIndexes.IsValueCreated)
                throw new InvalidOperationException("Can't add indexes after the list has been frozen.");

            _indexes.Add(index);
        }

        public void ConfigureIndexes(IEnumerable<IIndex> indexes = null, bool beginReindexingOutdated = true) {
            if (indexes == null)
                indexes = Indexes;

            foreach (var idx in indexes) {
                idx.Configure();

                //IIndicesOperationResponse response = null;
                //var templatedIndex = idx as ITimeSeriesIndex;
                //if (templatedIndex != null)
                //    response = Client.PutTemplate(idx.VersionedName, template => templatedIndex.ConfigureTemplate(template));
                //else if (!Client.IndexExists(idx.VersionedName).Exists)
                //    response = Client.CreateIndex(idx.VersionedName, descriptor => idx.Configure(descriptor));

                //Debug.Assert(response == null || response.IsValid, response?.ServerError != null ? response.ServerError.Error : "Error creating the index or template.");

                //// Add existing indexes to the alias.
                //if (!Client.AliasExists(idx.Name).Exists) {
                //    if (templatedIndex != null) {
                //        var indices = Client.IndicesStats().Indices.Where(kvp => kvp.Key.StartsWith(idx.VersionedName)).Select(kvp => kvp.Key).ToList();
                //        if (indices.Count > 0) {
                //            var descriptor = new AliasDescriptor();
                //            foreach (string name in indices)
                //                descriptor.Add(add => add.Index(name).Alias(idx.Name));

                //            response = Client.Alias(descriptor);
                //        }
                //    } else {
                //        response = Client.Alias(a => a.Add(add => add.Index(idx.VersionedName).Alias(idx.Name)));
                //    }

                //    Debug.Assert(response != null && response.IsValid, response?.ServerError != null ? response.ServerError.Error : "Error creating the alias.");
                //}

                //if (!beginReindexingOutdated)
                //    continue;

                //if (_workItemQueue == null || _lockProvider == null)
                //    throw new InvalidOperationException("Must specify work item queue and lock provider in order to reindex.");

                //int currentVersion = GetIndexVersion(idx);

                //// already on current version
                //if (currentVersion >= idx.Version || currentVersion < 1)
                //    continue;

                //var reindexWorkItem = new ReindexWorkItem {
                //    OldIndex = String.Concat(idx.Name, "-v", currentVersion),
                //    NewIndex = idx.VersionedName,
                //    Alias = idx.Name,
                //    DeleteOld = true
                //};

                //foreach (var type in idx.IndexTypes.OfType<IChildIndexType>())
                //    reindexWorkItem.ParentMaps.Add(new ParentMap { Type = type.Name, ParentPath = type.ParentPath });

                //bool isReindexing = _lockProvider.IsLockedAsync(String.Concat("reindex:", reindexWorkItem.Alias, reindexWorkItem.OldIndex, reindexWorkItem.NewIndex)).Result;
                //// already reindexing
                //if (isReindexing)
                //    continue;

                //// enqueue reindex to new version
                //_lockProvider.TryUsingAsync("enqueue-reindex", () => _workItemQueue.EnqueueAsync(reindexWorkItem), TimeSpan.Zero, CancellationToken.None).Wait();
            }
        }

        public void DeleteIndexes(IEnumerable<IIndex> indexes = null) {
            if (indexes == null)
                indexes = Indexes;

            foreach (var idx in indexes)
                idx.Delete();
        }

        public async Task ReindexAsync(IEnumerable<IIndex> indexes = null, Func<int, string, Task> progressCallbackAsync = null) {
            if (indexes == null)
                indexes = Indexes;

            // TODO: Base the progress on the number of indexes
            foreach (var idx in indexes)
                await idx.ReindexAsync(progressCallbackAsync).AnyContext();
        }
    }
}
