using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
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
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Configuration;

public class Index : IIndex
{
    private readonly Lazy<IElasticQueryBuilder> _queryBuilder;
    private readonly Lazy<ElasticQueryParser> _queryParser;
    private readonly Lazy<ElasticMappingResolver> _mappingResolver;
    private readonly Lazy<QueryFieldResolver> _fieldResolver;
    protected readonly ILogger _logger;
    private readonly IDictionary<string, ICustomFieldType> _customFieldTypes = new Dictionary<string, ICustomFieldType>();

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

    public string Name { get; protected set; }
    public bool HasMultipleIndexes { get; protected set; } = false;
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

        bool existsResult = await IndexExistsAsync(Name).AnyContext();
        if (existsResult)
        {
            _isEnsured = true;
            return;
        }

        await ConfigureAsync().AnyContext();
    }

    public virtual Task MaintainAsync(bool includeOptionalTasks = true)
    {
        return Task.CompletedTask;
    }

    public virtual IPromise<IAliases> ConfigureIndexAliases(AliasesDescriptor aliases)
    {
        return aliases;
    }

    public IElasticQueryBuilder QueryBuilder => _queryBuilder.Value;
    public ElasticQueryParser QueryParser => _queryParser.Value;
    public ElasticMappingResolver MappingResolver => _mappingResolver.Value;
    public QueryFieldResolver FieldResolver => _fieldResolver.Value;

    public int BulkBatchSize { get; set; } = 1000;

    public virtual Task DeleteAsync()
    {
        return DeleteIndexAsync(Name);
    }

    protected virtual async Task CreateIndexAsync(string name, Func<CreateIndexDescriptor, CreateIndexDescriptor> descriptor = null)
    {
        if (name == null)
            throw new ArgumentNullException(nameof(name));

        descriptor ??= ConfigureIndex;

        var response = await Configuration.Client.Indices.CreateAsync(name, descriptor).AnyContext();
        _isEnsured = true;

        // check for valid response or that the index already exists
        if (response.IsValid || response.ServerError?.Status == 400 &&
            response.ServerError.Error.Type is "index_already_exists_exception" or "resource_already_exists_exception")
        {
            _logger.LogRequest(response);
            return;
        }

        throw new RepositoryException(response.GetErrorMessage($"Error creating the index {name}"), response.OriginalException);
    }

    protected virtual async Task UpdateIndexAsync(string name, Func<UpdateIndexSettingsDescriptor, UpdateIndexSettingsDescriptor> descriptor = null)
    {
        var updateIndexDescriptor = new UpdateIndexSettingsDescriptor(name);
        if (descriptor != null)
        {
            updateIndexDescriptor = descriptor(updateIndexDescriptor);
        }
        else
        {
            var currentSettings = await Configuration.Client.Indices.GetSettingsAsync(name);
            var currentAnalyzers = currentSettings.Indices[name]?.Settings?.Analysis?.Analyzers ?? new Analyzers();
            var currentTokenizers = currentSettings.Indices[name]?.Settings?.Analysis?.Tokenizers ?? new Tokenizers();
            var currentTokenFilters = currentSettings.Indices[name]?.Settings?.Analysis?.TokenFilters ?? new TokenFilters();
            var currentNormalizers = currentSettings.Indices[name]?.Settings?.Analysis?.Normalizers ?? new Normalizers();
            var currentCharFilters = currentSettings.Indices[name]?.Settings?.Analysis?.CharFilters ?? new CharFilters();

            // default to update dynamic index settings from the ConfigureIndex method
            var createIndexDescriptor = new CreateIndexDescriptor(name);
            createIndexDescriptor = ConfigureIndex(createIndexDescriptor);
            var settings = ((IIndexState)createIndexDescriptor).Settings;

            // strip off non-dynamic index settings
            settings.FileSystemStorageImplementation = null;
            settings.NumberOfRoutingShards = null;
            settings.NumberOfShards = null;
            settings.Queries = null;
            settings.RoutingPartitionSize = null;
            settings.Hidden = null;
            settings.Sorting = null;
            settings.SoftDeletes = null;

            if (settings.Analysis?.Analyzers != null)
            {
                foreach (var analyzer in settings.Analysis.Analyzers.ToList())
                {
                    if (!currentAnalyzers.ContainsKey(analyzer.Key))
                        _logger.LogError("New analyzer {AnalyzerKey} can't be added to existing index", analyzer.Key);
                }
            }

            if (settings.Analysis?.Tokenizers != null)
            {
                foreach (var tokenizer in settings.Analysis.Tokenizers.ToList())
                {
                    if (!currentTokenizers.ContainsKey(tokenizer.Key))
                        _logger.LogError("New tokenizer {TokenizerKey}  can't be added to existing index", tokenizer.Key);
                }
            }

            if (settings.Analysis?.TokenFilters != null)
            {
                foreach (var tokenFilter in settings.Analysis.TokenFilters.ToList())
                {
                    if (!currentTokenFilters.ContainsKey(tokenFilter.Key))
                        _logger.LogError("New token filter {TokenFilterKey} can't be added to existing index", tokenFilter.Key);
                }
            }

            if (settings.Analysis?.Normalizers != null)
            {
                foreach (var normalizer in settings.Analysis.Normalizers.ToList())
                {
                    if (!currentNormalizers.ContainsKey(normalizer.Key))
                        _logger.LogError("New normalizer {NormalizerKey} can't be added to existing index", normalizer.Key);
                }
            }

            if (settings.Analysis?.CharFilters != null)
            {
                foreach (var charFilter in settings.Analysis.CharFilters.ToList())
                {
                    if (!currentCharFilters.ContainsKey(charFilter.Key))
                        _logger.LogError("New char filter {CharFilterKey} can't be added to existing index", charFilter.Key);
                }
            }

            settings.Analysis = null;

            updateIndexDescriptor.IndexSettings(_ => new NestPromise<IDynamicIndexSettings>(settings));
        }

        var response = await Configuration.Client.Indices.UpdateSettingsAsync(name, _ => updateIndexDescriptor).AnyContext();

        if (response.IsValid)
            _logger.LogRequest(response);
        else
            _logger.LogErrorRequest(response, $"Error updating index ({name}) settings");
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

        var response = await Configuration.Client.Indices.DeleteAsync(Indices.Index(names), i => i.IgnoreUnavailable()).AnyContext();

        if (response.IsValid)
        {
            _logger.LogRequest(response);
            return;
        }

        throw new RepositoryException(response.GetErrorMessage("Error deleting the index {names}"), response.OriginalException);
    }

    protected async Task<bool> IndexExistsAsync(string name)
    {
        if (name == null)
            throw new ArgumentNullException(nameof(name));

        var response = await Configuration.Client.Indices.ExistsAsync(name).AnyContext();
        if (response.ApiCall.Success)
        {
            _logger.LogRequest(response);
            return response.Exists;
        }

        throw new RepositoryException(response.GetErrorMessage($"Error checking to see if index {name} exists"), response.OriginalException);
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

    public virtual CreateIndexDescriptor ConfigureIndex(CreateIndexDescriptor idx)
    {
        return idx.Aliases(ConfigureIndexAliases);
    }

    public virtual void ConfigureSettings(ConnectionSettings settings) { }

    public virtual void Dispose() { }
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

    public virtual TypeMappingDescriptor<T> ConfigureIndexMapping(TypeMappingDescriptor<T> map)
    {
        return map.AutoMap<T>().Properties(p => p.SetupDefaults());
    }

    public override CreateIndexDescriptor ConfigureIndex(CreateIndexDescriptor idx)
    {
        idx = base.ConfigureIndex(idx);
        return idx.Map<T>(f =>
        {
            if (CustomFieldTypes.Count > 0)
            {
                f.DynamicTemplates(d =>
                {
                    foreach (var customFieldType in CustomFieldTypes.Values)
                        d.DynamicTemplate($"idx_{customFieldType.Type}", df => df.PathMatch("idx.*").Match($"{customFieldType.Type}-*").Mapping(customFieldType.ConfigureMapping));

                    return d;
                });
            }

            return ConfigureIndexMapping(f);
        });
    }

    protected override async Task UpdateIndexAsync(string name, Func<UpdateIndexSettingsDescriptor, UpdateIndexSettingsDescriptor> descriptor = null)
    {
        await base.UpdateIndexAsync(name, descriptor).AnyContext();

        var typeMappingDescriptor = new TypeMappingDescriptor<T>();
        typeMappingDescriptor = ConfigureIndexMapping(typeMappingDescriptor);
        var mapping = (ITypeMapping)typeMappingDescriptor;

        var response = await Configuration.Client.Indices.PutMappingAsync<T>(m =>
        {
            m.Properties(_ => new NestPromise<IProperties>(mapping.Properties));
            if (CustomFieldTypes.Count > 0)
            {
                m.DynamicTemplates(d =>
                {
                    foreach (var customFieldType in CustomFieldTypes.Values)
                        d.DynamicTemplate($"idx_{customFieldType.Type}", df => df.PathMatch("idx.*").Match($"{customFieldType.Type}-*").Mapping(customFieldType.ConfigureMapping));

                    return d;
                });
            }

            return m;
        }).AnyContext();

        if (response.IsValid)
            _logger.LogRequest(response);
        else
            _logger.LogErrorRequest(response, $"Error updating index ({name}) mappings.");
    }

    public override void ConfigureSettings(ConnectionSettings settings)
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

internal class NestPromise<TValue> : IPromise<TValue> where TValue : class
{
    public NestPromise(TValue value)
    {
        Value = value;
    }

    public TValue Value { get; }
}
