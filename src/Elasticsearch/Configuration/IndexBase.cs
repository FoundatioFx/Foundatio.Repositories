using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Logging;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Jobs;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Configuration {
    public abstract class IndexBase : IIndex {
        protected readonly IElasticClient _client;
        protected readonly ILogger _logger;
        private readonly List<IIndexType> _types = new List<IIndexType>();
        private readonly Lazy<IReadOnlyCollection<IIndexType>> _frozenTypes;

        public IndexBase(IElasticClient client, string name, ILoggerFactory loggerFactory = null) {
            Name = name;
            _client = client;
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
            IIndicesResponse response = null;

            if (_client.IndexExists(Name).Exists) {
                response = _client.DeleteIndex(Name);
                _logger.Trace(() => response.GetRequest());
            }

            if (response != null && !response.IsValid)
                throw new ApplicationException("An error occurred deleting the index: " + response.ServerError.Error, response.ConnectionStatus.OriginalException);
        }

        public virtual Task ReindexAsync(Func<int, string, Task> progressCallbackAsync = null) {
            var reindexer = new ElasticReindexer(_client, _logger);

            // TODO: Does reindexing to the same index work?
            var reindexWorkItem = new ReindexWorkItem {
                OldIndex = Name,
                NewIndex = Name,
                DeleteOld = false
            };

            foreach (var type in IndexTypes.OfType<IChildIndexType>())
                reindexWorkItem.ParentMaps.Add(new ParentMap { Type = type.Name, ParentPath = type.ParentPath });

            return reindexer.ReindexAsync(reindexWorkItem, progressCallbackAsync);
        }
    }
}