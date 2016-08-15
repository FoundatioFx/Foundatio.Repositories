using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Elasticsearch.Net.ConnectionPool;
using Foundatio.Caching;
using Foundatio.Jobs;
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

    public abstract class ElasticConfigurationBase: IElasticConfiguration {
        private readonly List<IIndex> _indexes = new List<IIndex>();
        private readonly Lazy<IReadOnlyCollection<IIndex>> _frozenIndexes;

        public ElasticConfigurationBase() : this(null, null, null) {}

        public ElasticConfigurationBase(IQueue<WorkItemData> workItemQueue, ICacheClient cacheClient, ILoggerFactory loggerFactory)
            : this((IElasticClient)null, workItemQueue, cacheClient, loggerFactory) {
        }

        public ElasticConfigurationBase(Uri serverUri, IQueue<WorkItemData> workItemQueue, ICacheClient cacheClient, ILoggerFactory loggerFactory)
            : this(new[] { serverUri }, workItemQueue, cacheClient, loggerFactory) {
        }

        public ElasticConfigurationBase(IEnumerable<Uri> serverUris, IQueue<WorkItemData> workItemQueue, ICacheClient cacheClient, ILoggerFactory loggerFactory)
            : this(new ElasticClient(new ConnectionSettings(new StaticConnectionPool(serverUris)).EnableTcpKeepAlive(30 * 1000, 2000)), workItemQueue, cacheClient, loggerFactory) {
        }

        public ElasticConfigurationBase(IElasticClient client, IQueue<WorkItemData> workItemQueue, ICacheClient cacheClient, ILoggerFactory loggerFactory) {
            Client = client;
            _frozenIndexes = new Lazy<IReadOnlyCollection<IIndex>>(() => _indexes.AsReadOnly());
        }

        protected void SetClient(Uri serverUri) {
            SetClient(new[] { serverUri });
        }

        protected void SetClient(IEnumerable<Uri> serverUris) {
            Client = new ElasticClient(new ConnectionSettings(new StaticConnectionPool(serverUris)).EnableTcpKeepAlive(30 * 1000, 2000));
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
