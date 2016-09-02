using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Elasticsearch.Net;
using Foundatio.Lock;
using Foundatio.Logging;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Jobs;
using Foundatio.Repositories.Extensions;
using Foundatio.Utility;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Configuration {
    public abstract class IndexBase : IIndex {
        protected readonly IElasticConfiguration _config;
        protected readonly ILockProvider _lockProvider;
        protected readonly ILogger _logger;
        private readonly List<IIndexType> _types = new List<IIndexType>();
        private readonly Lazy<IReadOnlyCollection<IIndexType>> _frozenTypes;

        public IndexBase(IElasticConfiguration elasticConfiguration, string name) {
            Name = name;
            _config = elasticConfiguration;
            _lockProvider = new CacheLockProvider(elasticConfiguration.Cache, elasticConfiguration.MessageBus, elasticConfiguration.LoggerFactory);
            _logger = elasticConfiguration.LoggerFactory.CreateLogger(GetType());
            _frozenTypes = new Lazy<IReadOnlyCollection<IIndexType>>(() => _types.AsReadOnly());
        }

        public string Name { get; }
        public IReadOnlyCollection<IIndexType> IndexTypes => _frozenTypes.Value;

        public virtual void AddType(IIndexType type) {
            if (_frozenTypes.IsValueCreated)
                throw new InvalidOperationException("Can't add index types after the list has been frozen.");

            _types.Add(type);
        }

        public IIndexType<T> AddDynamicType<T>(string name) where T : class {
            var indexType = new DynamicIndexType<T>(this, name);
            AddType(indexType);

            return indexType;
        }

        public abstract Task ConfigureAsync();

        public virtual Task DeleteAsync() {
            return DeleteIndexAsync(Name);
        }
        
        protected virtual async Task CreateIndexAsync(string name, Func<CreateIndexDescriptor, CreateIndexDescriptor> descriptor) {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            bool result = await _lockProvider.TryUsingAsync("create-index:" + name, async t => {
                if (await IndexExistsAsync(name).AnyContext()) {
                    var healthResponse = await _config.Client.ClusterHealthAsync(h => h
                        .Index(name)
                        .WaitForStatus(WaitForStatus.Yellow)
                        .Timeout("10s")).AnyContext();

                    if (!healthResponse.IsValid || (healthResponse.Status != "green" && healthResponse.Status != "yellow") || healthResponse.TimedOut)
                        throw new ApplicationException($"Index {name} exists but is unhealthy: {healthResponse.Status}.", healthResponse.ConnectionStatus.OriginalException);

                    return;
                }

                var response = await _config.Client.CreateIndexAsync(name, descriptor).AnyContext();
                _logger.Trace(() => response.GetRequest());

                if (response.IsValid) {
                    while (!await IndexExistsAsync(name).AnyContext())
                        SystemClock.Sleep(100);

                    var healthResponse = await _config.Client.ClusterHealthAsync(h => h
                        .Index(name)
                        .WaitForStatus(WaitForStatus.Yellow)
                        .Timeout("10s")).AnyContext();

                    if (!healthResponse.IsValid || (healthResponse.Status != "green" && healthResponse.Status != "yellow") || healthResponse.TimedOut)
                        throw new ApplicationException($"Index {name} is unhealthy: {healthResponse.Status}.", healthResponse.ConnectionStatus.OriginalException);

                    return;
                }

                string message = $"Error creating the index {name}: {response.GetErrorMessage()}";
                _logger.Error().Exception(response.ConnectionStatus.OriginalException).Message(message).Property("request", response.GetRequest()).Write();
                throw new ApplicationException(message, response.ConnectionStatus.OriginalException);
            }, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));

            if (!result)
                throw new ApplicationException($"Unable to acquire index creation lock for \"{name}\".");
        }

        protected virtual async Task DeleteIndexAsync(string name) {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            if (!await IndexExistsAsync(name).AnyContext())
                return;

            var response = await _config.Client.DeleteIndexAsync(i => i.Index(name)).AnyContext();
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

            var response = await _config.Client.IndexExistsAsync(name).AnyContext();
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

            var reindexer = new ElasticReindexer(_config.Client, _config.Cache, _logger);
            return reindexer.ReindexAsync(reindexWorkItem, progressCallbackAsync);
        }

        public virtual void ConfigureSettings(ConnectionSettings settings) {
            foreach (var type in IndexTypes) {
                settings.MapDefaultTypeIndices(m => m[type.Type] = Name);
                settings.MapDefaultTypeNames(m => m[type.Type] = type.Name);
            }
        }

        public virtual void Dispose() {
            foreach (var indexType in IndexTypes)
                indexType.Dispose();
        }
    }
}