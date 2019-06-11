using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
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
    public class Index : IIndex {
        private readonly ConcurrentDictionary<string, PropertyInfo> _cachedProperties = new ConcurrentDictionary<string, PropertyInfo>(StringComparer.OrdinalIgnoreCase);
        private readonly Lazy<IElasticQueryBuilder> _queryBuilder;
        private readonly Lazy<ElasticQueryParser> _queryParser;
        private readonly Lazy<QueryFieldResolver> _fieldResolver;
        protected readonly ILockProvider _lockProvider;
        protected readonly ILogger _logger;

        public Index(IElasticConfiguration configuration, string name = null) {
            Name = name;
            Configuration = configuration;
            _queryBuilder = new Lazy<IElasticQueryBuilder>(CreateQueryBuilder);
            _queryParser = new Lazy<ElasticQueryParser>(CreateQueryParser);
            _fieldResolver = new Lazy<QueryFieldResolver>(CreateQueryFieldResolver);
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
                config.SetLoggerFactory(Configuration.LoggerFactory);
                config.UseFieldResolver(CreateQueryFieldResolver());
                config.UseNested();
                config.UseMappings(Configuration.Client, Name);
                Configuration.ConfigureGlobalQueryParsers(config);
                ConfigureQueryParser(config);
            });

            return parser;
        }

        protected virtual QueryFieldResolver CreateQueryFieldResolver() => null;

        protected virtual void ConfigureQueryParser(ElasticQueryParserConfiguration config) { }

        public string Name { get; protected set; }
        public bool HasMultipleIndexes { get; protected set; } = false;
        public ISet<string> AllowedQueryFields { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        public ISet<string> AllowedAggregationFields { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        public ISet<string> AllowedSortFields { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        public IElasticConfiguration Configuration { get; }

        public virtual string CreateDocumentId(object document) {
            switch (document) {
                case null:
                    throw new ArgumentNullException(nameof(document));
                case IIdentity identityDoc when !String.IsNullOrEmpty(identityDoc.Id):
                    return identityDoc.Id;
                case IHaveCreatedDate createdDoc when createdDoc.CreatedUtc != DateTime.MinValue:
                    return ObjectId.GenerateNewId(createdDoc.CreatedUtc).ToString();
                default:
                    return ObjectId.GenerateNewId().ToString();
            }
        }

        private string[] _indexes;
        public virtual string[] GetIndexesByQuery(IRepositoryQuery query) {
            return _indexes ?? (_indexes = new[] { Name });
        }

        public virtual string GetIndex(object target) {
            return Name;
        }

        public virtual Task ConfigureAsync() {
            return CreateIndexAsync(Name, ConfigureIndex);
        }

        public virtual Task EnsureIndexAsync(object target) {
            return Task.CompletedTask;
        }

        public virtual Task MaintainAsync(bool includeOptionalTasks = true) {
            return Task.CompletedTask;
        }

        public virtual IPromise<IAliases> ConfigureIndexAliases(AliasesDescriptor aliases) {
            return aliases;
        }

        public IElasticQueryBuilder QueryBuilder => _queryBuilder.Value;
        public ElasticQueryParser QueryParser => _queryParser.Value;
        public QueryFieldResolver FieldResolver => _fieldResolver.Value;

        public int DefaultCacheExpirationSeconds { get; set; } = RepositoryConstants.DEFAULT_CACHE_EXPIRATION_SECONDS;
        public int BulkBatchSize { get; set; } = 1000;

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

        protected virtual Task DeleteIndexAsync(string name) {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            return DeleteIndexesAsync(new[] { name });
        }

        protected virtual async Task DeleteIndexesAsync(string[] names) {
            if (names == null || names.Length == 0)
                throw new ArgumentNullException(nameof(names));

            var response = await Configuration.Client.DeleteIndexAsync(Indices.Index(names), i => i.IgnoreUnavailable()).AnyContext();
            if (_logger.IsEnabled(Microsoft.Extensions.Logging.LogLevel.Trace))
                _logger.LogTrace(response.GetRequest());

            if (response.IsValid)
                return;

            string message = $"Error deleting index {names}: {response.GetErrorMessage()}";
            _logger.LogWarning(response.OriginalException, message);
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

        protected virtual string GetTimeStampField() {
            return "updated";
        }

        public virtual CreateIndexDescriptor ConfigureIndex(CreateIndexDescriptor idx) {
            return idx.Aliases(ConfigureIndexAliases);
        }

        public virtual void ConfigureSettings(ConnectionSettings settings) {}

        public virtual void Dispose() {}
    }

    public class Index<T> : Index where T : class {
        private readonly string _typeName = typeof(T).Name.ToLower();

        public Index(IElasticConfiguration configuration, string name = null) : base(configuration, name) {
            Name = name ?? _typeName;
        }
        
        public virtual ITypeMapping ConfigureIndexMapping(TypeMappingDescriptor<T> map) {
            return map.AutoMap<T>().Properties(p => p.SetupDefaults());
        }

        public override CreateIndexDescriptor ConfigureIndex(CreateIndexDescriptor idx) {
            idx = base.ConfigureIndex(idx);
            return idx.Map<T>(ConfigureIndexMapping);
        }

        public override void ConfigureSettings(ConnectionSettings settings) {
            //settings.DefaultMappingFor<T>(d => d.IndexName(Name));
        }
    }
}