using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Fluent;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Clients.Elasticsearch.Mapping;
using Foundatio.AsyncEx;
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
using Foundatio.Utility;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Foundatio.Repositories.Elasticsearch.Configuration;

public class Index : IIndexCompatibility, IHaveLogger
{
    private readonly Lazy<IElasticQueryBuilder> _queryBuilder;
    private readonly Lazy<ElasticQueryParser> _queryParser;
    private readonly Lazy<ElasticMappingResolver> _mappingResolver;
    private readonly Lazy<QueryFieldResolver?> _fieldResolver;
    private readonly ConcurrentDictionary<string, ICustomFieldType> _customFieldTypes = new();
    private readonly AsyncLock _lock = new();
    private readonly CancellationTokenSource _disposedCancellationTokenSource = new();
    private int _disposed;
    protected readonly ILogger _logger;

    public Index(IElasticConfiguration configuration, string name)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);

        Name = name;
        Configuration = configuration;
        _queryBuilder = new Lazy<IElasticQueryBuilder>(CreateQueryBuilder);
        _queryParser = new Lazy<ElasticQueryParser>(CreateQueryParser);
        _mappingResolver = new Lazy<ElasticMappingResolver>(CreateMappingResolver);
        _fieldResolver = new Lazy<QueryFieldResolver?>(CreateQueryFieldResolver);
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

    protected virtual QueryFieldResolver? CreateQueryFieldResolver() => null;

    protected virtual void ConfigureQueryParser(ElasticQueryParserConfiguration config) { }

    public string Name { get; init; }
    public bool HasMultipleIndexes { get; init; } = false;
    public ISet<string> AllowedQueryFields { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
    public ISet<string> AllowedAggregationFields { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
    public ISet<string> AllowedSortFields { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
    public IElasticConfiguration Configuration { get; }
    public ILogger Logger => _logger;

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

    private string[]? _indexes;
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
    public virtual async Task EnsureIndexAsync(object? target)
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
    public QueryFieldResolver? FieldResolver => _fieldResolver.Value;

    public int BulkBatchSize { get; set; } = 1000;

    /// <summary>
    /// The number of documents Elasticsearch reads and writes per internal bulk batch while reindexing via
    /// <see cref="ReindexAsync"/>. Defaults to null, which uses the Elasticsearch reindex API default of 1000.
    /// Lower this if reindexing large documents triggers "rejected execution of coordinating operation" errors
    /// from indexing pressure limits.
    /// </summary>
    public int? ReindexBatchSize { get; set; }

    /// <summary>
    /// Throttles <see cref="ReindexAsync"/> to approximately this many documents per second. Defaults to null,
    /// which uses the Elasticsearch reindex API default of unlimited. Combine with <see cref="ReindexBatchSize"/>
    /// to reduce load on a cluster that is rejecting reindex requests due to indexing pressure limits.
    /// </summary>
    public float? ReindexRequestsPerSecond { get; set; }

    public virtual async Task DeleteAsync()
    {
        using (await _lock.LockAsync(_disposedCancellationTokenSource.Token).AnyContext())
        {
            await DeleteIndexAsync(Name).AnyContext();
            _isEnsured = false;
        }
    }

    protected virtual async Task CreateIndexAsync(string name, Action<CreateIndexRequestDescriptor>? descriptor = null)
    {
        if (name == null)
            throw new ArgumentNullException(nameof(name));

        descriptor ??= d => ConfigureIndex(d);

        var response = await Configuration.Client.Indices.CreateAsync((IndexName)name, descriptor).AnyContext();
        _logger.LogRequest(response);

        if (response.IsValidResponse || response.ElasticsearchServerError?.Status == 400 &&
            response.ElasticsearchServerError!.Error?.Type is "index_already_exists_exception" or "resource_already_exists_exception")
        {
            _isEnsured = true;
            return;
        }

        _logger.LogErrorRequest(response, "Error creating the index {Name}", name);
        throw new RepositoryException(response.GetErrorMessage($"Error creating the index {name}"), response.OriginalException());
    }

    protected virtual async Task UpdateIndexAsync(string name, Action<PutIndicesSettingsRequestDescriptor>? descriptor = null)
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

        var currentSettings = await Configuration.Client.Indices.GetSettingsAsync((Indices)name).AnyContext();
        if (!currentSettings.IsValidResponse)
        {
            _logger.LogErrorRequest(currentSettings, "Error getting index settings for {Name}", name);
            throw new RepositoryException(currentSettings.GetErrorMessage($"Error getting index settings for {name}"), currentSettings.OriginalException());
        }

        currentSettings.Settings.TryGetValue(name, out var indexState);
        if (indexState is null && currentSettings.Settings.Count is 1)
            indexState = currentSettings.Settings.Values.Single();
        else if (indexState is null)
            throw new RepositoryException($"Index name '{name}' resolved to {currentSettings.Settings.Count} indexes while updating settings; expected exactly one.");

        // GetSettingsAsync nests analysis settings under the "index" key (Settings.Index.Analysis); the root
        // Settings.Analysis is the write-time shape used in create requests and is not populated on reads. Read
        // from Settings.Index.Analysis so the diff below sees the components that already exist on the live index.
        var currentAnalysis = indexState?.Settings?.Index?.Analysis;

        // default to update dynamic index settings from the ConfigureIndex method
        var createIndexRequestDescriptor = new CreateIndexRequestDescriptor((IndexName)name);
        ConfigureIndex(createIndexRequestDescriptor);
        CreateIndexRequest createRequest = createIndexRequestDescriptor;
        var settings = createRequest.Settings;
        if (settings is null)
            return;

        // strip off non-dynamic index settings
        settings.Store = null;
        settings.NumberOfRoutingShards = null;
        settings.NumberOfShards = null;
        settings.Queries = null;
        settings.RoutingPartitionSize = null;
        settings.Hidden = null;
        settings.Sort = null;
        settings.SoftDeletes = null;

        WarnOnNewAnalysisComponents(settings.Analysis?.Analyzers?.Select(kvp => kvp.Key), currentAnalysis?.Analyzers?.Select(kvp => kvp.Key), "analyzer");
        WarnOnNewAnalysisComponents(settings.Analysis?.Tokenizers?.Select(kvp => kvp.Key), currentAnalysis?.Tokenizers?.Select(kvp => kvp.Key), "tokenizer");
        WarnOnNewAnalysisComponents(settings.Analysis?.TokenFilters?.Select(kvp => kvp.Key), currentAnalysis?.TokenFilters?.Select(kvp => kvp.Key), "token filter");
        WarnOnNewAnalysisComponents(settings.Analysis?.Normalizers?.Select(kvp => kvp.Key), currentAnalysis?.Normalizers?.Select(kvp => kvp.Key), "normalizer");
        WarnOnNewAnalysisComponents(settings.Analysis?.CharFilters?.Select(kvp => kvp.Key), currentAnalysis?.CharFilters?.Select(kvp => kvp.Key), "char filter");

        var updateResponse = await Configuration.Client.Indices.PutSettingsAsync(name, d => d.Reopen().Settings(settings)).AnyContext();

        if (updateResponse.IsValidResponse)
            _logger.LogRequest(updateResponse);
        else
            _logger.LogErrorRequest(updateResponse, $"Error updating index ({name}) settings");
    }

    private void WarnOnNewAnalysisComponents(IEnumerable<string>? desiredKeys, IEnumerable<string>? currentKeys, string componentType)
    {
        if (desiredKeys is null)
            return;

        var existing = currentKeys?.ToHashSet() ?? new HashSet<string>();
        foreach (string key in desiredKeys.Where(key => !existing.Contains(key)))
            _logger.LogWarning("Adding new {ComponentType} {ComponentKey} to existing index (requires close/reopen)", componentType, key);
    }

    protected virtual Task DeleteIndexAsync(string name)
    {
        if (name == null)
            throw new ArgumentNullException(nameof(name));

        return DeleteIndexesAsync(new[] { name });
    }

    protected virtual async Task DeleteIndexesAsync(string[] names, Func<string, bool>? indexFilter = null)
    {
        if (names == null || names.Length == 0)
            throw new ArgumentNullException(nameof(names));

        // Resolve wildcards and aliases to their concrete backing indexes; use GetAsync because
        // ResolveIndexAsync is broken in ES 9.x client, and Elasticsearch rejects deleting an index by alias name.
        var getResponse = await Configuration.Client.Indices.GetAsync(Indices.Parse(String.Join(",", names)), d => d
            .LimitToNamesAndAliases()
            .AllowNoIndices()
            .ExpandWildcards(ExpandWildcard.All)
            .IgnoreUnavailable()).AnyContext();
        if (!getResponse.IsValidResponse && getResponse.ElasticsearchServerError?.Status is not 404)
        {
            _logger.LogErrorRequest(getResponse, "Error resolving indexes {Names}", String.Join(", ", names));
            throw new RepositoryException(getResponse.GetErrorMessage($"Error resolving indexes {String.Join(",", names)}"), getResponse.OriginalException());
        }

        IEnumerable<string> resolvedNames = getResponse.Indices?.Keys.Select(k => k.ToString()) ?? [];
        if (indexFilter is not null)
            resolvedNames = resolvedNames.Where(indexFilter);

        var indexNames = resolvedNames.ToList();
        if (indexNames.Count == 0)
            return;

        // Batch delete to avoid HTTP line too long errors (ES default max is 4096 bytes)
        // Each index name is roughly 30-50 bytes, so we batch in groups of 50
        const int batchSize = 50;
        foreach (var batch in indexNames.Chunk(batchSize))
        {
            var response = await Configuration.Client.Indices.DeleteAsync(Indices.Parse(String.Join(",", batch)), i => i.IgnoreUnavailable()).AnyContext();

            if (response.IsValidResponse)
            {
                _logger.LogRequest(response);
                continue;
            }

            // Another caller may delete a concrete index after the metadata lookup above. The request is
            // explicitly ignore-unavailable, so preserve that contract even when the client surfaces the 404
            // as an invalid response.
            if (response.ElasticsearchServerError?.Status is 404)
                continue;

            _logger.LogErrorRequest(response, "Error deleting the index {Indexes}", String.Join(",", batch));
            throw new RepositoryException(response.GetErrorMessage($"Error deleting the index {String.Join(",", batch)}"), response.OriginalException());
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

        if (response.ApiCallDetails.HttpStatusCode is 404)
        {
            _logger.LogRequest(response);
            return false;
        }

        _logger.LogErrorRequest(response, "Error checking to see if index {Name} exists", name);
        throw new RepositoryException(response.GetErrorMessage($"Error checking to see if index {name} exists"), response.OriginalException());
    }

    public virtual Task ReindexAsync(Func<int, string?, Task>? progressCallbackAsync = null)
    {
        var reindexWorkItem = new ReindexWorkItem
        {
            OldIndex = Name,
            NewIndex = Name,
            Alias = Name,
            DeleteOld = false,
            TimestampField = GetTimeStampField(),
            ReindexBatchSize = ReindexBatchSize,
            ReindexRequestsPerSecond = ReindexRequestsPerSecond
        };

        var reindexer = new ElasticReindexer(Configuration.Client, Configuration.Serializer, _logger);
        return reindexer.ReindexAsync(reindexWorkItem, progressCallbackAsync);
    }

    internal static int? ParseCreatedMajor(string? created, string? createdString)
    {
        if (!String.IsNullOrEmpty(createdString))
        {
            int dotIndex = createdString.IndexOf('.');
            string majorPart = dotIndex > 0 ? createdString[..dotIndex] : createdString;
            if (Int32.TryParse(majorPart, out int major) && major > 0)
                return major;
        }

        if (!String.IsNullOrEmpty(created) && Int64.TryParse(created, out long createdId))
        {
            long major = createdId / 1_000_000;
            if (major is > 0 and <= Int32.MaxValue)
                return (int)major;
        }

        return null;
    }

    /// <summary>
    /// Returns the stable index or alias name that resolves every physical index currently backing this index.
    /// </summary>
    protected virtual string GetCompatibilityIndexPattern()
    {
        // The reindexer writes copy failures to a sibling "<name>-error" physical index without aliases, so it
        // must be discovered by name or compatibility preflight would silently skip an old-format partition.
        return $"{Name},{String.Concat(Name, "-error")}";
    }

    /// <summary>
    /// Checks the Elasticsearch version compatibility of every physical index currently backing this index. This
    /// issues a single <c>GET</c> request for the pattern returned by <see cref="GetCompatibilityIndexPattern"/>.
    /// </summary>
    /// <param name="cancellationToken">The token used to cancel the server-info or index-settings request.</param>
    public virtual async Task<IReadOnlyCollection<IndexCompatibilityInfo>> GetIndexCompatibilityAsync(CancellationToken cancellationToken = default)
    {
        var serverVersion = await GetServerVersionAsync(cancellationToken).AnyContext();
        int serverMajor = serverVersion.Major;

        string pattern = GetCompatibilityIndexPattern();

        // Ask for settings rather than aliases so index.version.created comes back in this same response; the
        // response is keyed by concrete index name, so aliases in the pattern resolve and de-duplicate for free.
        var response = await Configuration.Client.Indices.GetAsync(Indices.Parse(pattern), d => d.LimitToIndexSettings().ExpandWildcards(ExpandWildcard.All).IgnoreUnavailable(), cancellationToken).AnyContext();
        if (!response.IsValidResponse)
        {
            if (response.ElasticsearchServerError?.Status is 404)
                return [];

            _logger.LogErrorRequest(response, "Error getting indexes matching {Pattern} while checking Elasticsearch version compatibility", pattern);
            throw new RepositoryException(response.GetErrorMessage($"Error getting indexes matching {pattern} while checking Elasticsearch version compatibility"), response.OriginalException());
        }

        _logger.LogRequest(response);

        if (response.Indices is null || response.Indices.Count == 0)
            return [];

        var infos = new List<IndexCompatibilityInfo>(response.Indices.Count);
        foreach (var kvp in response.Indices)
        {
            var versioning = kvp.Value?.Settings?.Index?.Version;
            int? createdMajor = ParseCreatedMajor(versioning?.Created, versioning?.CreatedString);
            if (!createdMajor.HasValue)
                throw new RepositoryException($"Unable to determine the Elasticsearch version that created index '{kvp.Key}'.");

            infos.Add(new IndexCompatibilityInfo
            {
                Name = kvp.Key,
                CreatedMajor = createdMajor.Value,
                CreatedVersion = versioning?.CreatedString,
                ServerMajor = serverMajor,
                ServerVersion = serverVersion.Version
            });
        }

        return infos;
    }

    internal virtual bool OwnsCompatibilityIndex(string sourceIndex)
    {
        string canonicalName = CompatibilityIndexName.GetCanonicalName(CompatibilityIndexName.StripErrorSuffix(sourceIndex), Name);
        return String.Equals(canonicalName, Name, StringComparison.Ordinal);
    }

    /// <summary>
    /// Returns whether this configured index owns <paramref name="sourceIndex"/> and no other configured index
    /// claims it more strongly. A family that contains the name natively (without stripping a generated
    /// compatibility prefix) outranks a family that only matches after stripping; among native families the
    /// longest configured name wins. This keeps a legitimately named index such as <c>reindexed-v8-events</c>
    /// from being claimed — and later reindexed or deleted — by a configuration named <c>events</c>.
    /// </summary>
    internal bool OwnsCompatibilityIndexExclusively(string sourceIndex)
    {
        if (!OwnsCompatibilityIndex(sourceIndex))
            return false;

        foreach (IIndex other in Configuration.Indexes)
        {
            if (ReferenceEquals(other, this) || other is not Index otherIndex)
                continue;

            if (!otherIndex.OwnsCompatibilityIndex(sourceIndex))
                continue;

            bool mineNative = IsNativeFamilyMember(sourceIndex, Name);
            bool theirsNative = IsNativeFamilyMember(sourceIndex, otherIndex.Name);
            if (theirsNative && (!mineNative || otherIndex.Name.Length > Name.Length))
                return false;
        }

        return true;
    }

    private static bool IsNativeFamilyMember(string indexName, string configuredName)
    {
        if (String.IsNullOrEmpty(indexName) || String.IsNullOrEmpty(configuredName))
            return false;

        return String.Equals(indexName, configuredName, StringComparison.Ordinal)
            || indexName.StartsWith($"{configuredName}-", StringComparison.Ordinal);
    }

    internal virtual void ValidateCompatibilityUpgradeSource(string sourceIndex, bool ownsLogicalAlias)
    {
        if (!OwnsCompatibilityIndexExclusively(sourceIndex))
            throw new RepositoryException($"Index '{sourceIndex}' does not belong to configured index '{Name}'.");
    }

    private async Task<(int Major, string Version)> GetServerVersionAsync(CancellationToken cancellationToken)
    {
        var response = await Configuration.Client.InfoAsync(cancellationToken).AnyContext();
        _logger.LogRequest(response);
        if (!response.IsValidResponse)
            throw new RepositoryException(response.GetErrorMessage("Unable to determine the current Elasticsearch server version while checking index compatibility."), response.OriginalException());

        string? version = response.Version?.Number;
        int? major = ParseCreatedMajor(null, version);
        if (!major.HasValue)
            throw new RepositoryException("Unable to determine the current Elasticsearch server version while checking index compatibility.");

        return (major.Value, version!);
    }

    protected virtual string? GetTimeStampField()
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
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        _disposedCancellationTokenSource.Cancel();
        _disposedCancellationTokenSource.Dispose();
    }
}

public class Index<T> : Index, IIndex<T> where T : class
{
    private static readonly string _typeName = typeof(T).Name.ToLower();

    public Index(IElasticConfiguration configuration, string? name = null) : base(configuration, name ?? _typeName)
    {
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

    protected override async Task UpdateIndexAsync(string name, Action<PutIndicesSettingsRequestDescriptor>? descriptor = null)
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

    protected override string? GetTimeStampField()
    {
        if (typeof(IHaveDates).IsAssignableFrom(typeof(T)))
            return InferField(f => ((IHaveDates)f).UpdatedUtc);

        if (typeof(IHaveCreatedDate).IsAssignableFrom(typeof(T)))
            return InferField(f => ((IHaveCreatedDate)f).CreatedUtc);

        return null;
    }

    public Inferrer Infer => Configuration.Client.Infer;
    public string InferField(Expression<Func<T, object?>> objectPath) => Infer.Field(objectPath);
    public string InferPropertyName(Expression<Func<T, object?>> objectPath) => Infer.PropertyName(objectPath);
}
