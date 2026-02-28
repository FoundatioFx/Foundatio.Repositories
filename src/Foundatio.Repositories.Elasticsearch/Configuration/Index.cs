using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.AsyncEx;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Analysis;
using Elastic.Clients.Elasticsearch.Fluent;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Clients.Elasticsearch.Mapping;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Foundatio.Repositories.Elasticsearch.CustomFields;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Jobs;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Utility;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Foundatio.Repositories.Elasticsearch.Configuration;

public class Index : IIndex
{
    private readonly Lazy<IElasticQueryBuilder> _queryBuilder;
    private readonly Lazy<ElasticQueryParser> _queryParser;
    private readonly Lazy<ElasticMappingResolver> _mappingResolver;
    private readonly Lazy<QueryFieldResolver> _fieldResolver;
    private readonly ConcurrentDictionary<string, ICustomFieldType> _customFieldTypes = new();
    private readonly AsyncLock _lock = new();
    private readonly CancellationTokenSource _disposedCancellationTokenSource = new();
    private bool _disposed;
    protected readonly ILogger _logger;

    public Index(IElasticConfiguration configuration, string name = null)
    {
        Name = name;
        Configuration = configuration;
        _queryBuilder = new Lazy<IElasticQueryBuilder>(CreateQueryBuilder);
        _queryParser = new Lazy<ElasticQueryParser>(CreateQueryParser);
        _mappingResolver = new Lazy<ElasticMappingResolver>(CreateMappingResolver);
        _fieldResolver = new Lazy<QueryFieldResolver>(CreateQueryFieldResolver);
        _logger = configuration.LoggerFactory?.CreateLogger(GetType()) ?? NullLogger.Instance;
    }

    protected void AddStandardCustomFieldTypes()
    {
        AddCustomFieldType<BooleanFieldType>();
        AddCustomFieldType<DateFieldType>();
        AddCustomFieldType<DoubleFieldType>();
        AddCustomFieldType<FloatFieldType>();
        AddCustomFieldType<IntegerFieldType>();
        AddCustomFieldType<KeywordFieldType>();
        AddCustomFieldType<LongFieldType>();
        AddCustomFieldType<StringFieldType>();
    }

    protected void AddCustomFieldType(ICustomFieldType customFieldType)
    {
        _customFieldTypes[customFieldType.Type] = customFieldType;
    }

    protected void AddCustomFieldType<TFieldType>() where TFieldType : ICustomFieldType, new()
    {
        var fieldType = new TFieldType();
        _customFieldTypes[fieldType.Type] = fieldType;
    }

    public IDictionary<string, ICustomFieldType> CustomFieldTypes => _customFieldTypes;

    protected virtual IElasticQueryBuilder CreateQueryBuilder()
    {
        var builder = new ElasticQueryBuilder();
        builder.UseQueryParser(_queryParser.Value);
        Configuration.ConfigureGlobalQueryBuilders(builder);
        ConfigureQueryBuilder(builder);

        return builder;
    }

    protected virtual void ConfigureQueryBuilder(ElasticQueryBuilder builder) { }

    protected virtual ElasticMappingResolver CreateMappingResolver()
    {
        return ElasticMappingResolver.Create(Configuration.Client, Name, _logger);
    }

    protected virtual ElasticQueryParser CreateQueryParser()
    {
        var parser = new ElasticQueryParser(config =>
        {
            config.SetLoggerFactory(Configuration.LoggerFactory);
            config.UseFieldResolver(_fieldResolver.Value);
            config.UseNested();
            config.UseMappings(_mappingResolver.Value);
            Configuration.ConfigureGlobalQueryParsers(config);
            ConfigureQueryParser(config);
        });

        return parser;
    }

    protected virtual QueryFieldResolver CreateQueryFieldResolver() => null;

    protected virtual void ConfigureQueryParser(ElasticQueryParserConfiguration config) { }

    public string Name { get; init; }
    public bool HasMultipleIndexes { get; init; } = false;
    public ISet<string> AllowedQueryFields { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
    public ISet<string> AllowedAggregationFields { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
    public ISet<string> AllowedSortFields { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
    public IElasticConfiguration Configuration { get; }

    public virtual string CreateDocumentId(object document)
    {
        return document switch
        {
            null => throw new ArgumentNullException(nameof(document)),
            IIdentity identityDoc when !String.IsNullOrEmpty(identityDoc.Id) => identityDoc.Id,
            IHaveCreatedDate createdDoc when createdDoc.CreatedUtc != DateTime.MinValue => ObjectId.GenerateNewId(createdDoc.CreatedUtc).ToString(),
            _ => ObjectId.GenerateNewId(Configuration.TimeProvider.GetUtcNow().UtcDateTime).ToString(),
        };
    }

    private string[] _indexes;
    public virtual string[] GetIndexesByQuery(IRepositoryQuery query)
    {
        return _indexes ??= new[] { Name };
    }

    public virtual string GetIndex(object target)
    {
        return Name;
    }

    public virtual Task ConfigureAsync()
    {
        return ConfigureAsync(Name);
    }

    protected virtual async Task ConfigureAsync(string name)
    {
        if (!await IndexExistsAsync(name).AnyContext())
            await CreateIndexAsync(name).AnyContext();
        else
            await UpdateIndexAsync(name).AnyContext();
    }

    private bool _isEnsured = false;
    public virtual async Task EnsureIndexAsync(object target)
    {
        if (_isEnsured)
            return;

        using (await _lock.LockAsync(_disposedCancellationTokenSource.Token).AnyContext())
        {
            if (_isEnsured)
                return;

            await ConfigureAsync().AnyContext();
            _isEnsured = true;
        }
    }

    public virtual Task MaintainAsync(bool includeOptionalTasks = true)
    {
        return Task.CompletedTask;
    }

    public virtual void ConfigureIndexAliases(FluentDictionaryOfNameAlias fluentDictionaryOfNameAlias)
    {
    }

    public IElasticQueryBuilder QueryBuilder => _queryBuilder.Value;
    public ElasticQueryParser QueryParser => _queryParser.Value;
    public ElasticMappingResolver MappingResolver => _mappingResolver.Value;
    public QueryFieldResolver FieldResolver => _fieldResolver.Value;

    public int BulkBatchSize { get; set; } = 1000;

    public virtual async Task DeleteAsync()
    {
        using (await _lock.LockAsync(_disposedCancellationTokenSource.Token).AnyContext())
        {
            await DeleteIndexAsync(Name).AnyContext();
            _isEnsured = false;
        }
    }

    protected virtual async Task CreateIndexAsync(string name, Action<CreateIndexRequestDescriptor> descriptor = null)
    {
        if (name == null)
            throw new ArgumentNullException(nameof(name));

        descriptor ??= d => ConfigureIndex(d);

        var response = await Configuration.Client.Indices.CreateAsync((IndexName)name, descriptor).AnyContext();
        _logger.LogRequest(response);
        _isEnsured = true;

        // check for valid response or that the index already exists
        if (response.IsValidResponse || response.ElasticsearchServerError?.Status == 400 &&
            response.ElasticsearchServerError.Error.Type is "index_already_exists_exception" or "resource_already_exists_exception")
        {
            return;
        }

        throw new RepositoryException(response.GetErrorMessage($"Error creating the index {name}"), response.OriginalException());
    }

    protected virtual async Task UpdateIndexAsync(string name, Action<PutIndicesSettingsRequestDescriptor> descriptor = null)
    {
        if (descriptor != null)
        {
            var response = await Configuration.Client.Indices.PutSettingsAsync(name, descriptor).AnyContext();

            if (response.IsValidResponse)
                _logger.LogRequest(response);
            else
                _logger.LogErrorRequest(response, $"Error updating index ({name}) settings");
            return;
        }

        var currentSettings = await Configuration.Client.Indices.GetSettingsAsync((Indices)name);
        var indexState = currentSettings.Settings.TryGetValue(name, out var indexSettings) ? indexSettings : null;
        var currentAnalyzers = indexState?.Settings?.Analysis?.Analyzers ?? new Analyzers();
        var currentTokenizers = indexState?.Settings?.Analysis?.Tokenizers ?? new Tokenizers();
        var currentTokenFilters = indexState?.Settings?.Analysis?.TokenFilters ?? new TokenFilters();
        var currentNormalizers = indexState?.Settings?.Analysis?.Normalizers ?? new Normalizers();
        var currentCharFilters = indexState?.Settings?.Analysis?.CharFilters ?? new CharFilters();

        // default to update dynamic index settings from the ConfigureIndex method
        var createIndexRequestDescriptor = new CreateIndexRequestDescriptor((IndexName)name);
        ConfigureIndex(createIndexRequestDescriptor);
        CreateIndexRequest createRequest = createIndexRequestDescriptor;
        var settings = createRequest.Settings;

        // strip off non-dynamic index settings
        settings.Store = null;
        settings.NumberOfRoutingShards = null;
        settings.NumberOfShards = null;
        settings.Queries = null;
        settings.RoutingPartitionSize = null;
        settings.Hidden = null;
        settings.Sort = null;
        settings.SoftDeletes = null;

        if (settings.Analysis?.Analyzers != null && currentAnalyzers != null)
        {
            var currentKeys = currentAnalyzers.Select(kvp => kvp.Key).ToHashSet();
            foreach (var analyzer in settings.Analysis.Analyzers.ToList())
            {
                if (!currentKeys.Contains(analyzer.Key))
                    _logger.LogError("New analyzer {AnalyzerKey} can't be added to existing index", analyzer.Key);
            }
        }

        if (settings.Analysis?.Tokenizers != null && currentTokenizers != null)
        {
            var currentKeys = currentTokenizers.Select(kvp => kvp.Key).ToHashSet();
            foreach (var tokenizer in settings.Analysis.Tokenizers.ToList())
            {
                if (!currentKeys.Contains(tokenizer.Key))
                    _logger.LogError("New tokenizer {TokenizerKey}  can't be added to existing index", tokenizer.Key);
            }
        }

        if (settings.Analysis?.TokenFilters != null && currentTokenFilters != null)
        {
            var currentKeys = currentTokenFilters.Select(kvp => kvp.Key).ToHashSet();
            foreach (var tokenFilter in settings.Analysis.TokenFilters.ToList())
            {
                if (!currentKeys.Contains(tokenFilter.Key))
                    _logger.LogError("New token filter {TokenFilterKey} can't be added to existing index", tokenFilter.Key);
            }
        }

        if (settings.Analysis?.Normalizers != null && currentNormalizers != null)
        {
            var currentKeys = currentNormalizers.Select(kvp => kvp.Key).ToHashSet();
            foreach (var normalizer in settings.Analysis.Normalizers.ToList())
            {
                if (!currentKeys.Contains(normalizer.Key))
                    _logger.LogError("New normalizer {NormalizerKey} can't be added to existing index", normalizer.Key);
            }
        }

        if (settings.Analysis?.CharFilters != null && currentCharFilters != null)
        {
            var currentKeys = currentCharFilters.Select(kvp => kvp.Key).ToHashSet();
            foreach (var charFilter in settings.Analysis.CharFilters.ToList())
            {
                if (!currentKeys.Contains(charFilter.Key))
                    _logger.LogError("New char filter {CharFilterKey} can't be added to existing index", charFilter.Key);
            }
        }

        settings.Analysis = null;

        var updateResponse = await Configuration.Client.Indices.PutSettingsAsync(name, d => d.Reopen().Settings(settings)).AnyContext();

        if (updateResponse.IsValidResponse)
            _logger.LogRequest(updateResponse);
        else
            _logger.LogErrorRequest(updateResponse, $"Error updating index ({name}) settings");
    }

    protected virtual Task DeleteIndexAsync(string name)
    {
        if (name == null)
            throw new ArgumentNullException(nameof(name));

        return DeleteIndexesAsync(new[] { name });
    }

    protected virtual async Task DeleteIndexesAsync(string[] names)
    {
        if (names == null || names.Length == 0)
            throw new ArgumentNullException(nameof(names));

        // Resolve wildcards to actual index names to avoid issues with action.destructive_requires_name=true.
        // Note: ResolveIndexAsync sends a body in ES 9.x client which ES rejects; use GetAsync instead.
        var indexNames = new List<string>();
        foreach (var name in names)
        {
            if (name.Contains("*") || name.Contains("?"))
            {
                var getResponse = await Configuration.Client.Indices.GetAsync(Indices.Parse(name), d => d.IgnoreUnavailable()).AnyContext();
                if (getResponse.IsValidResponse && getResponse.Indices != null)
                {
                    foreach (var kvp in getResponse.Indices)
                        indexNames.Add(kvp.Key);
                }
            }
            else
            {
                indexNames.Add(name);
            }
        }

        if (indexNames.Count == 0)
            return;

        // Batch delete to avoid HTTP line too long errors (ES default max is 4096 bytes)
        // Each index name is roughly 30-50 bytes, so we batch in groups of 50
        const int batchSize = 50;
        foreach (var batch in indexNames.Chunk(batchSize))
        {
            var response = await Configuration.Client.Indices.DeleteAsync(Indices.Parse(string.Join(",", batch)), i => i.IgnoreUnavailable()).AnyContext();

            if (response.IsValidResponse)
            {
                _logger.LogRequest(response);
                continue;
            }

            throw new RepositoryException(response.GetErrorMessage($"Error deleting the index {names}"), response.OriginalException());
        }
    }

    protected async Task<bool> IndexExistsAsync(string name)
    {
        if (name == null)
            throw new ArgumentNullException(nameof(name));

        var response = await Configuration.Client.Indices.ExistsAsync(name).AnyContext();
        if (response.ApiCallDetails.HasSuccessfulStatusCode)
        {
            _logger.LogRequest(response);
            return response.Exists;
        }

        throw new RepositoryException(response.GetErrorMessage($"Error checking to see if index {name} exists"), response.OriginalException());
    }

    public virtual Task ReindexAsync(Func<int, string, Task> progressCallbackAsync = null)
    {
        var reindexWorkItem = new ReindexWorkItem
        {
            OldIndex = Name,
            NewIndex = Name,
            DeleteOld = false,
            TimestampField = GetTimeStampField()
        };

        var reindexer = new ElasticReindexer(Configuration.Client, _logger);
        return reindexer.ReindexAsync(reindexWorkItem, progressCallbackAsync);
    }

    protected virtual string GetTimeStampField()
    {
        return null;
    }

    public virtual void ConfigureIndex(CreateIndexRequestDescriptor idx)
    {
        idx.Aliases(ConfigureIndexAliases);
    }

    public virtual void ConfigureSettings(ElasticsearchClientSettings settings) { }

    public virtual void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        _disposedCancellationTokenSource.Cancel();
        _disposedCancellationTokenSource.Dispose();
    }
}

public class Index<T> : Index, IIndex<T> where T : class
{
    private readonly string _typeName = typeof(T).Name.ToLower();

    public Index(IElasticConfiguration configuration, string name = null) : base(configuration, name)
    {
        Name = name ?? _typeName;
    }

    protected override ElasticMappingResolver CreateMappingResolver()
    {
        return ElasticMappingResolver.Create<T>(ConfigureIndexMapping, Configuration.Client, Name, _logger);
    }

    public virtual void ConfigureIndexMapping(TypeMappingDescriptor<T> map)
    {
        map.Properties(p => p.SetupDefaults());
    }

    public override void ConfigureIndex(CreateIndexRequestDescriptor idx)
    {
        base.ConfigureIndex(idx);
        idx.Mappings<T>(f =>
        {
            if (CustomFieldTypes.Count > 0)
            {
                f.DynamicTemplates(d =>
                {
                    foreach (var customFieldType in CustomFieldTypes.Values)
                        d.Add($"idx_{customFieldType.Type}", df => df.PathMatch("idx.*").Match($"{customFieldType.Type}-*").Mapping(customFieldType.ConfigureMapping<T>()));
                });
            }

            ConfigureIndexMapping(f);
        });
    }

    protected override async Task UpdateIndexAsync(string name, Action<PutIndicesSettingsRequestDescriptor> descriptor = null)
    {
        await base.UpdateIndexAsync(name, descriptor).AnyContext();

        var typeMappingDescriptor = new TypeMappingDescriptor<T>();
        ConfigureIndexMapping(typeMappingDescriptor);
        var mapping = (TypeMapping)typeMappingDescriptor;

        var response = await Configuration.Client.Indices.PutMappingAsync<T>(m =>
        {
            m.Properties(mapping.Properties);
            if (CustomFieldTypes.Count > 0)
            {
                m.DynamicTemplates(d =>
                {
                    foreach (var customFieldType in CustomFieldTypes.Values)
                        d.Add($"idx_{customFieldType.Type}", df => df.PathMatch("idx.*").Match($"{customFieldType.Type}-*").Mapping(customFieldType.ConfigureMapping<T>()));
                });
            }
        }).AnyContext();

        if (response.IsValidResponse)
            _logger.LogRequest(response);
        else
            _logger.LogErrorRequest(response, $"Error updating index ({name}) mappings.");
    }

    public override void ConfigureSettings(ElasticsearchClientSettings settings)
    {
        settings.DefaultMappingFor<T>(d => d.IndexName(Name));
    }

    protected override string GetTimeStampField()
    {
        if (typeof(IHaveDates).IsAssignableFrom(typeof(T)))
            return InferField(f => ((IHaveDates)f).UpdatedUtc);

        if (typeof(IHaveCreatedDate).IsAssignableFrom(typeof(T)))
            return InferField(f => ((IHaveCreatedDate)f).CreatedUtc);

        return null;
    }

    public Inferrer Infer => Configuration.Client.Infer;
    public string InferField(Expression<Func<T, object>> objectPath) => Infer.Field(objectPath);
    public string InferPropertyName(Expression<Func<T, object>> objectPath) => Infer.PropertyName(objectPath);
}
