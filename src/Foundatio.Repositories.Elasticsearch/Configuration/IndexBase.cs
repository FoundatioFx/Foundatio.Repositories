using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Lock;
using Foundatio.Logging;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Jobs;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Models;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Configuration {
    public abstract class IndexBase : IIndex {
        protected readonly ILockProvider _lockProvider;
        protected readonly ILogger _logger;
        private readonly List<IIndexType> _types = new List<IIndexType>();
        private readonly Lazy<IReadOnlyCollection<IIndexType>> _frozenTypes;

        public IndexBase(IElasticConfiguration configuration, string name) {
            Name = name;
            Configuration = configuration;
            _lockProvider = new CacheLockProvider(configuration.Cache, configuration.MessageBus, configuration.LoggerFactory);
            _logger = configuration.LoggerFactory.CreateLogger(GetType());
            _frozenTypes = new Lazy<IReadOnlyCollection<IIndexType>>(() => _types.AsReadOnly());
        }

        public string Name { get; }
        public IElasticConfiguration Configuration { get; }
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

        public virtual async Task ConfigureAsync() {
            foreach (var t in IndexTypes)
                await t.ConfigureAsync().AnyContext();
        }

        public virtual Task DeleteAsync() {
            return DeleteIndexAsync(Name);
        }

        protected virtual async Task CreateIndexAsync(string name, Func<CreateIndexDescriptor, CreateIndexDescriptor> descriptor) {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            var response = await Configuration.Client.CreateIndexAsync(name, descriptor).AnyContext();
            _logger.Info(() => response.GetRequest());

            // check for valid response or that the index already exists
            if (response.IsValid || response.ServerError.Status == 400 && response.ServerError.Error.Type == "index_already_exists_exception")
                return;

            string message = $"Error creating the index {name}: {response.GetErrorMessage()}";
            _logger.Error().Exception(response.OriginalException).Message(message).Property("request", response.GetRequest()).Write();
            throw new ApplicationException(message, response.OriginalException);
        }

        protected virtual async Task DeleteIndexAsync(string name) {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            if (!await IndexExistsAsync(name).AnyContext())
                return;

            var response = await Configuration.Client.DeleteIndexAsync(name).AnyContext();
            _logger.Trace(() => response.GetRequest());

            if (response.IsValid)
                return;

            string message = $"Error deleting index {name}: {response.GetErrorMessage()}";
            _logger.Error().Exception(response.OriginalException).Message(message).Property("request", response.GetRequest()).Write();
            throw new ApplicationException(message, response.OriginalException);
        }

        protected async Task<bool> IndexExistsAsync(string name) {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            var response = await Configuration.Client.IndexExistsAsync(name).AnyContext();
            if (response.IsValid)
                return response.Exists;

            string message = $"Error checking to see if index {name} exists: {response.GetErrorMessage()}";
            _logger.Error().Exception(response.OriginalException).Message(message).Property("request", response.GetRequest()).Write();
            throw new ApplicationException(message, response.OriginalException);
        }

        public virtual Task ReindexAsync(Func<int, string, Task> progressCallbackAsync = null) {
            var reindexWorkItem = new ReindexWorkItem {
                OldIndex = Name,
                NewIndex = Name,
                DeleteOld = false,
                TimestampField = GetTimeStampField()
            };

            var reindexer = new ElasticReindexer(Configuration.Client, _logger);
            return reindexer.ReindexAsync(reindexWorkItem, progressCallbackAsync);
        }

        /// <summary>
        /// Attempt to get the document modified date for reindexing.
        /// NOTE: We make the assumption that all types implement the same date interfaces.
        /// </summary>
        protected virtual string GetTimeStampField() {
            if (IndexTypes.Count == 0)
                return null;

            var type = IndexTypes.First().Type;
            if (IndexTypes.All(i => typeof(IHaveDates).IsAssignableFrom(i.Type)))
                return Configuration.Client.Infer.PropertyName(type.GetProperty(nameof(IHaveDates.UpdatedUtc)));

            if (IndexTypes.All(i => typeof(IHaveCreatedDate).IsAssignableFrom(i.Type)))
                return Configuration.Client.Infer.PropertyName(type.GetProperty(nameof(IHaveCreatedDate.CreatedUtc)));

            return null;
        }

        public virtual CreateIndexDescriptor ConfigureIndex(CreateIndexDescriptor idx) {
            var aliases = new AliasesDescriptor();
            var mappings = new MappingsDescriptor();

            foreach (var t in IndexTypes) {
                aliases = t.ConfigureIndexAliases(aliases);
                mappings = t.ConfigureIndexMappings(mappings);
            }

            return idx.Aliases(a => aliases).Mappings(m => mappings);
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