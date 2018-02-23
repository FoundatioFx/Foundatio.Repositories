using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Caching;
using Foundatio.Jobs;
using Foundatio.Lock;
using Foundatio.Messaging;
using Foundatio.Queues;
using Foundatio.Repositories.Extensions;
using Nest;
using System.Threading;
using Elasticsearch.Net;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Parsers.ElasticQueries;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Foundatio.Repositories.Elasticsearch.Configuration {
    public interface IElasticConfiguration : IDisposable {
        IElasticClient Client { get; }
        ICacheClient Cache { get; }
        IMessageBus MessageBus { get; }
        ILoggerFactory LoggerFactory { get; }
        IReadOnlyCollection<IIndex> Indexes { get; }
        IIndexType<T> GetIndexType<T>() where T : class;
        IIndexType GetIndexType(Type type);
        IIndex GetIndex(string name);
        void ConfigureGlobalQueryBuilders(ElasticQueryBuilder builder);
        void ConfigureGlobalQueryParsers(ElasticQueryParserConfiguration config);
        Task ConfigureIndexesAsync(IEnumerable<IIndex> indexes = null, bool beginReindexingOutdated = true);
        Task MaintainIndexesAsync(IEnumerable<IIndex> indexes = null);
        Task DeleteIndexesAsync(IEnumerable<IIndex> indexes = null);
        Task ReindexAsync(IEnumerable<IIndex> indexes = null, Func < int, string, Task> progressCallbackAsync = null);
    }

    public class ElasticConfiguration: IElasticConfiguration {
        protected readonly IQueue<WorkItemData> _workItemQueue;
        protected readonly ILogger _logger;
        protected readonly ILockProvider _beginReindexLockProvider;
        protected readonly ILockProvider _lockProvider;
        private readonly List<IIndex> _indexes = new List<IIndex>();
        private readonly Lazy<IReadOnlyCollection<IIndex>> _frozenIndexes;
        private readonly Lazy<IElasticClient> _client;
        protected readonly bool _shouldDisposeCache;

        public ElasticConfiguration(IQueue<WorkItemData> workItemQueue = null, ICacheClient cacheClient = null, IMessageBus messageBus = null, ILoggerFactory loggerFactory = null) {
            _workItemQueue = workItemQueue;
            LoggerFactory = loggerFactory ?? NullLoggerFactory.Instance;
            _logger = LoggerFactory.CreateLogger(GetType());
            Cache = cacheClient ?? new InMemoryCacheClient(new InMemoryCacheClientOptions { LoggerFactory = loggerFactory });
            _lockProvider = new CacheLockProvider(Cache, messageBus, loggerFactory);
            _beginReindexLockProvider = new ThrottlingLockProvider(Cache, 1, TimeSpan.FromMinutes(15));
            _shouldDisposeCache = cacheClient == null;
            MessageBus = messageBus ?? new InMemoryMessageBus(new InMemoryMessageBusOptions { LoggerFactory = loggerFactory });
            _frozenIndexes = new Lazy<IReadOnlyCollection<IIndex>>(() => _indexes.AsReadOnly());
            _client = new Lazy<IElasticClient>(CreateElasticClient);
        }

        protected virtual IElasticClient CreateElasticClient() {
            var settings = new ConnectionSettings(CreateConnectionPool() ?? new SingleNodeConnectionPool(new Uri("http://localhost:9200")));
            ConfigureSettings(settings);
            foreach (var index in Indexes)
                index.ConfigureSettings(settings);

            return new ElasticClient(settings);
        }

        public virtual void ConfigureGlobalQueryBuilders(ElasticQueryBuilder builder) {}

        public virtual void ConfigureGlobalQueryParsers(ElasticQueryParserConfiguration config) {}

        protected virtual void ConfigureSettings(ConnectionSettings settings) {
            settings.EnableTcpKeepAlive(TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(2));
        }

        protected virtual IConnectionPool CreateConnectionPool() {
            return null;
        }

        public IElasticClient Client => _client.Value;
        public ICacheClient Cache { get; }
        public IMessageBus MessageBus { get; }
        public ILoggerFactory LoggerFactory { get; }
        public IReadOnlyCollection<IIndex> Indexes => _frozenIndexes.Value;

        public IIndexType<T> GetIndexType<T>() where T : class {
            return GetIndexType(typeof(T)) as IIndexType<T>;
        }

        public IIndexType GetIndexType(Type type) {
            foreach (var index in Indexes)
                foreach (var indexType in index.IndexTypes)
                    if (indexType.Type == type)
                        return indexType;

            return null;
        }

        public IIndex GetIndex(string name) {
            foreach (var index in Indexes)
                if (index.Name == name)
                    return index;

            return null;
        }

        public void AddIndex(IIndex index) {
            if (_frozenIndexes.IsValueCreated)
                throw new InvalidOperationException("Can't add indexes after the list has been frozen.");

            _indexes.Add(index);
        }

        public async Task ConfigureIndexesAsync(IEnumerable<IIndex> indexes = null, bool beginReindexingOutdated = true) {
            if (indexes == null)
                indexes = Indexes;

            foreach (var idx in indexes) {
                await idx.ConfigureAsync().AnyContext();
                if (idx is IMaintainableIndex maintainableIndex)
                    await maintainableIndex.MaintainAsync(includeOptionalTasks: false).AnyContext();

                if (!beginReindexingOutdated)
                    continue;

                if (_workItemQueue == null || _beginReindexLockProvider == null)
                    throw new InvalidOperationException("Must specify work item queue and lock provider in order to reindex.");

                if (!(idx is VersionedIndex versionedIndex))
                    continue;

                int currentVersion = await versionedIndex.GetCurrentVersionAsync().AnyContext();
                if (versionedIndex.Version <= currentVersion)
                    continue;

                var reindexWorkItem = versionedIndex.CreateReindexWorkItem(currentVersion);
                bool isReindexing = await _lockProvider.IsLockedAsync(String.Join(":", "reindex", reindexWorkItem.Alias, reindexWorkItem.OldIndex, reindexWorkItem.NewIndex)).AnyContext();
                if (isReindexing)
                    continue;

                // enqueue reindex to new version, only allowed every 15 minutes
                string enqueueReindexLockName = String.Join(":", "enqueue-reindex", reindexWorkItem.Alias, reindexWorkItem.OldIndex, reindexWorkItem.NewIndex);
                await _beginReindexLockProvider.TryUsingAsync(enqueueReindexLockName, () => _workItemQueue.EnqueueAsync(reindexWorkItem), TimeSpan.Zero, new CancellationToken(true)).AnyContext();
            }
        }

        public async Task MaintainIndexesAsync(IEnumerable<IIndex> indexes = null) {
            if (indexes == null)
                indexes = Indexes;

            foreach (var idx in indexes.OfType<IMaintainableIndex>())
                await idx.MaintainAsync().AnyContext();
        }

        public Task DeleteIndexesAsync(IEnumerable<IIndex> indexes = null) {
            if (indexes == null)
                indexes = Indexes;

            var tasks = new List<Task>();
            foreach (var idx in indexes)
                tasks.Add(idx.DeleteAsync());

            return Task.WhenAll(tasks);
        }

        public async Task ReindexAsync(IEnumerable<IIndex> indexes = null, Func<int, string, Task> progressCallbackAsync = null) {
            if (indexes == null)
                indexes = Indexes;

            // TODO: Base the progress on the number of indexes
            foreach (var idx in indexes)
                await idx.ReindexAsync(progressCallbackAsync).AnyContext();
        }

        public virtual void Dispose() {
            if (_shouldDisposeCache)
                Cache.Dispose();

            foreach (var index in Indexes)
                index.Dispose();
        }
    }
}
