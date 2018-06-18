using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Utility;
using Nest;
using System.Linq.Expressions;
using System.Reflection;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Lock;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Foundatio.Repositories.Extensions;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Jobs;

namespace Foundatio.Repositories.Elasticsearch.Configuration {
    public class Index<T> : IIndex<T> where T : class {
        protected static readonly bool HasIdentity = typeof(IIdentity).IsAssignableFrom(typeof(T));
        protected static readonly bool HasCreatedDate = typeof(IHaveCreatedDate).IsAssignableFrom(typeof(T));
        private readonly string _typeName = typeof(T).Name.ToLower();
        private readonly ConcurrentDictionary<string, PropertyInfo> _cachedProperties = new ConcurrentDictionary<string, PropertyInfo>(StringComparer.OrdinalIgnoreCase);
        private readonly Lazy<IElasticQueryBuilder> _queryBuilder;
        private readonly Lazy<ElasticQueryParser> _queryParser;
        private readonly Lazy<AliasMap> _aliasMap;
        protected readonly ILockProvider _lockProvider;
        protected readonly ILogger _logger;

        public Index(IElasticConfiguration configuration, string name = null, Consistency defaultConsistency = Consistency.Eventual) {
            Name = name ?? _typeName;
            Type = typeof(T);
            Configuration = configuration;
            DefaultConsistency = defaultConsistency;
            _queryBuilder = new Lazy<IElasticQueryBuilder>(CreateQueryBuilder);
            _queryParser = new Lazy<ElasticQueryParser>(CreateQueryParser);
            _aliasMap = new Lazy<AliasMap>(GetAliasMap);
            _lockProvider = new CacheLockProvider(configuration.Cache, configuration.MessageBus, configuration.LoggerFactory);
            _logger = configuration.LoggerFactory?.CreateLogger(GetType()) ?? NullLogger.Instance;
        }

        protected virtual IElasticQueryBuilder CreateQueryBuilder() {
            var builder = new ElasticQueryBuilder();
            builder.UseQueryParser(_queryParser.Value);
            Configuration.ConfigureGlobalQueryBuilders(builder);
            ConfigureQueryBuilder(builder);

            return builder;
        }

        protected virtual void ConfigureQueryBuilder(ElasticQueryBuilder builder) {}

        protected virtual ElasticQueryParser CreateQueryParser() {
            var parser = new ElasticQueryParser(config => {
                config.UseMappings(this);
                config.UseNested();
                Configuration.ConfigureGlobalQueryParsers(config);
                ConfigureQueryParser(config);
            });

            return parser;
        }

        protected virtual void ConfigureQueryParser(ElasticQueryParserConfiguration config) { }

        public string Name { get; }
        public Type Type { get; }
        public Consistency DefaultConsistency { get; }
        public ISet<string> AllowedQueryFields { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        public ISet<string> AllowedAggregationFields { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        public ISet<string> AllowedSortFields { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        public IElasticConfiguration Configuration { get; }

        public virtual string CreateDocumentId(T document) {
            if (document == null)
                throw new ArgumentNullException(nameof(document));

            if (HasIdentity) {
                string id = ((IIdentity)document).Id;
                if (!String.IsNullOrEmpty(id))
                    return id;
            }

            if (HasCreatedDate) {
                var date = ((IHaveCreatedDate)document).CreatedUtc;
                if (date != DateTime.MinValue)
                    return ObjectId.GenerateNewId(date).ToString();
            }

            return ObjectId.GenerateNewId().ToString();
        }

        public virtual Task ConfigureAsync() {
            return CreateIndexAsync(Name, ConfigureIndex);
        }

        public virtual AliasesDescriptor ConfigureIndexAliases(AliasesDescriptor aliases) {
            return aliases;
        }

        public virtual MappingsDescriptor ConfigureIndexMappings(MappingsDescriptor mappings) {
            return mappings.Map<T>(ElasticConfiguration.DocType, BuildMapping);
        }

        public virtual TypeMappingDescriptor<T> BuildMapping(TypeMappingDescriptor<T> map) {
            return map.Properties(p => p.SetupDefaults());
        }

        public IElasticQueryBuilder QueryBuilder => _queryBuilder.Value;
        public ElasticQueryParser QueryParser => _queryParser.Value;
        public AliasMap AliasMap => _aliasMap.Value;

        public int DefaultCacheExpirationSeconds { get; set; } = RepositoryConstants.DEFAULT_CACHE_EXPIRATION_SECONDS;
        public int BulkBatchSize { get; set; } = 1000;

        private AliasMap GetAliasMap() {
            var visitor = new AliasMappingVisitor(Configuration.Client.Infer);
            var walker = new MappingWalker(visitor);
            var descriptor = BuildMapping(new TypeMappingDescriptor<T>());
            walker.Accept(descriptor);

            return visitor.RootAliasMap;
        }

        public string GetFieldName(Field field) {
            var result = AliasMap?.Resolve(field.Name);
            if (!String.IsNullOrEmpty(result?.Name))
                return result.Name;

            if (!String.IsNullOrEmpty(field.Name))
                field = GetPropertyInfo(field.Name) ?? field;

            return Configuration.Client.Infer.Field(field);
        }

        private PropertyInfo GetPropertyInfo(string property) {
            return _cachedProperties.GetOrAdd(property, s => Type.GetProperty(property, BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.Instance));
        }

        public string GetFieldName(Expression<Func<T, object>> objectPath) => Configuration.Client.Infer.Field(objectPath);
        public string GetPropertyName(PropertyName property) => Configuration.Client.Infer.PropertyName(property);
        public string GetPropertyName(Expression<Func<T, object>> objectPath) => Configuration.Client.Infer.PropertyName(objectPath);

        public virtual Task DeleteAsync() {
            return DeleteIndexAsync(Name);
        }

        protected virtual async Task CreateIndexAsync(string name, Func<CreateIndexDescriptor, CreateIndexDescriptor> descriptor) {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            var response = await Configuration.Client.CreateIndexAsync(name, descriptor).AnyContext();
            if (_logger.IsEnabled(Microsoft.Extensions.Logging.LogLevel.Trace))
                _logger.LogInformation(response.GetRequest());

            // check for valid response or that the index already exists
            if (response.IsValid || response.ServerError.Status == 400 && response.ServerError.Error.Type == "index_already_exists_exception")
                return;

            string message = $"Error creating the index {name}: {response.GetErrorMessage()}";
            _logger.LogError(response.OriginalException, message);
            throw new ApplicationException(message, response.OriginalException);
        }

        protected virtual async Task DeleteIndexAsync(string name) {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            if (!await IndexExistsAsync(name).AnyContext())
                return;

            var response = await Configuration.Client.DeleteIndexAsync(name).AnyContext();
            if (_logger.IsEnabled(Microsoft.Extensions.Logging.LogLevel.Trace))
                _logger.LogTrace(response.GetRequest());

            if (response.IsValid)
                return;

            string message = $"Error deleting index {name}: {response.GetErrorMessage()}";
            _logger.LogError(response.OriginalException, message);
            throw new ApplicationException(message, response.OriginalException);
        }

        protected async Task<bool> IndexExistsAsync(string name) {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            var response = await Configuration.Client.IndexExistsAsync(name).AnyContext();
            if (response.IsValid)
                return response.Exists;

            string message = $"Error checking to see if index {name} exists: {response.GetErrorMessage()}";
            _logger.LogError(response.OriginalException, message);
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
            if (typeof(IHaveDates).IsAssignableFrom(Type))
                return Configuration.Client.Infer.PropertyName(Type.GetProperty(nameof(IHaveDates.UpdatedUtc)));

            if (typeof(IHaveCreatedDate).IsAssignableFrom(Type))
                return Configuration.Client.Infer.PropertyName(Type.GetProperty(nameof(IHaveCreatedDate.CreatedUtc)));

            return null;
        }

        public virtual CreateIndexDescriptor ConfigureIndex(CreateIndexDescriptor idx) {
            var aliases = new AliasesDescriptor();
            var mappings = new MappingsDescriptor();

            aliases = ConfigureIndexAliases(aliases);
            mappings = ConfigureIndexMappings(mappings);

            return idx.Aliases(a => aliases).Mappings(m => mappings);
        }

        public virtual void ConfigureSettings(ConnectionSettings settings) {
            settings.MapDefaultTypeIndices(m => m[Type] = Name);
            settings.MapDefaultTypeNames(m => m[Type] = ElasticConfiguration.DocType);
        }

        public virtual void Dispose() {}
    }
}