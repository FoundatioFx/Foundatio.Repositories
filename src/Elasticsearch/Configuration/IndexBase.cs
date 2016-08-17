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

namespace Foundatio.Repositories.Elasticsearch.Configuration {
    public abstract class IndexBase : IIndex {
        protected readonly IElasticClient _client;
        protected readonly ICacheClient _cache;
        protected readonly bool _shouldDisposeCache;
        protected readonly ILogger _logger;
        private readonly List<IIndexType> _types = new List<IIndexType>();
        private readonly Lazy<IReadOnlyCollection<IIndexType>> _frozenTypes;

        public IndexBase(IElasticClient client, string name, ICacheClient cache = null, ILoggerFactory loggerFactory = null) {
            Name = name;
            _client = client;
            _cache = cache;
            _shouldDisposeCache = cache == null;
            _logger = loggerFactory.CreateLogger(GetType());
            _frozenTypes = new Lazy<IReadOnlyCollection<IIndexType>>(() => _types.AsReadOnly());
        }

        public string Name { get; }
        public IReadOnlyCollection<IIndexType> IndexTypes => _frozenTypes.Value;

        public virtual void AddType(IIndexType type) {
            if (_frozenTypes.IsValueCreated)
                throw new InvalidOperationException("Can't add index types after the list has been frozen.");

            _types.Add(type);
        }

        public abstract Task ConfigureAsync();

        public virtual Task DeleteAsync() {
            return DeleteIndexAsync(Name);
        }
        
        protected virtual async Task CreateIndexAsync(string name, Func<CreateIndexDescriptor, CreateIndexDescriptor> descriptor) {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            if (await IndexExistsAsync(name).AnyContext()) {
                var healthResponse = await _client.ClusterHealthAsync(h => h.Index(name).WaitForStatus(WaitForStatus.Green)).AnyContext();
                if (!healthResponse.IsValid)
                    throw new ApplicationException($"Index {name} exists but is unhealthy: {healthResponse.Status}.", healthResponse.ConnectionStatus.OriginalException);

                return;
            }

            var response = await _client.CreateIndexAsync(name, descriptor).AnyContext();
            _logger.Trace(() => response.GetRequest());

            if (response.IsValid) {
                while (!await IndexExistsAsync(name).AnyContext())
                    SystemClock.Sleep(100);

                var healthResponse = await _client.ClusterHealthAsync(h => h.Index(name).WaitForStatus(WaitForStatus.Green)).AnyContext();
                if (!healthResponse.IsValid)
                    throw new ApplicationException($"Index {name} is unhealthy: {healthResponse.Status}.", healthResponse.ConnectionStatus.OriginalException);

                return;
            }
            
            string message = $"Error creating the index {name}: {response.GetErrorMessage()}";
            _logger.Error().Exception(response.ConnectionStatus.OriginalException).Message(message).Property("request", response.GetRequest()).Write();
            throw new ApplicationException(message, response.ConnectionStatus.OriginalException);
        }

        protected virtual async Task DeleteIndexAsync(string name) {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            if (!await IndexExistsAsync(name).AnyContext())
                return;

            var response = await _client.DeleteIndexAsync(i => i.Index(name)).AnyContext();
            _logger.Trace(() => response.GetRequest());

            if (response.IsValid) {
                while (await IndexExistsAsync(name).AnyContext())
                    SystemClock.Sleep(100);
                
                return;
            }
            
            string message = $"Error deleting index {name}: {response.GetErrorMessage()}";
            _logger.Error().Exception(response.ConnectionStatus.OriginalException).Message(message).Property("request", response.GetRequest()).Write();
            throw new ApplicationException(message, response.ConnectionStatus.OriginalException);
        }
        
        protected async Task<bool> IndexExistsAsync(string name) {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            var response = await _client.IndexExistsAsync(name).AnyContext();
            if (response.IsValid)
                return response.Exists;

            string message = $"Error checking to see if index {name} exists: {response.GetErrorMessage()}";
            _logger.Error().Exception(response.ConnectionStatus.OriginalException).Message(message).Property("request", response.GetRequest()).Write();
            throw new ApplicationException(message, response.ConnectionStatus.OriginalException);
        }

        public virtual Task ReindexAsync(Func<int, string, Task> progressCallbackAsync = null) {
            var reindexWorkItem = new ReindexWorkItem {
                OldIndex = Name,
                NewIndex = Name,
                DeleteOld = false
            };

            foreach (var type in IndexTypes.OfType<IChildIndexType>())
                reindexWorkItem.ParentMaps.Add(new ParentMap { Type = type.Name, ParentPath = type.ParentPath });

            var reindexer = new ElasticReindexer(_client, _cache, _logger);
            return reindexer.ReindexAsync(reindexWorkItem, progressCallbackAsync);
        }

        public virtual void Dispose() {
            foreach (var indexType in IndexTypes)
                indexType.Dispose();
        }
    }
}