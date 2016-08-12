using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Caching;
using Foundatio.Logging;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Jobs;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Configuration {
    public abstract class IndexBase : IIndex {
        protected readonly IElasticClient _client;
        protected readonly ICacheClient _cache;
        protected readonly ILogger _logger;
        private readonly List<IIndexType> _types = new List<IIndexType>();
        private readonly Lazy<IReadOnlyCollection<IIndexType>> _frozenTypes;

        public IndexBase(IElasticClient client, string name, ICacheClient cache = null, ILoggerFactory loggerFactory = null) {
            Name = name;
            _client = client;
            _cache = cache ?? new NullCacheClient();
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

        public abstract void Configure();

        public virtual void Delete() {
            DeleteIndex(Name);
        }

        protected virtual void DeleteIndex(string name) {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            if (!_client.IndexExists(name).Exists)
                return;

            var response = _client.DeleteIndex(name);
            _logger.Trace(() => response.GetRequest());

            if (response.IsValid)
                return;

            string message = $"Error deleting index {name}: {response.GetErrorMessage()}";
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
    }
}