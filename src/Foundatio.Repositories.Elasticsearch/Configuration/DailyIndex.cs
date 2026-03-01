using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Clients.Elasticsearch.Mapping;
using Exceptionless.DateTimeExtensions;
using Foundatio.Caching;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Jobs;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;
using Foundatio.Repositories.Utility;
using Microsoft.Extensions.Logging;

namespace Foundatio.Repositories.Elasticsearch.Configuration;

public class DailyIndex : VersionedIndex
{
    protected static readonly CultureInfo EnUs = new("en-US");
    private readonly List<IndexAliasAge> _aliases = new();
    private readonly Lazy<IReadOnlyCollection<IndexAliasAge>> _frozenAliases;
    private readonly ICacheClient _aliasCache;
    private TimeSpan? _maxIndexAge;
    protected readonly Func<object, DateTime> _getDocumentDateUtc;
    protected readonly string[] _defaultIndexes;
    private readonly ConcurrentDictionary<DateTime, string> _ensuredDates = new();

    public DailyIndex(IElasticConfiguration configuration, string name, int version = 1, Func<object, DateTime> getDocumentDateUtc = null)
        : base(configuration, name, version)
    {
        AddAlias(Name);
        _frozenAliases = new Lazy<IReadOnlyCollection<IndexAliasAge>>(() => _aliases.AsReadOnly());
        _aliasCache = new ScopedCacheClient(configuration.Cache, "alias");
        _getDocumentDateUtc = getDocumentDateUtc;
        _defaultIndexes = [Name];
        HasMultipleIndexes = true;

        if (_getDocumentDateUtc != null)
            _getDocumentDateUtc = document =>
            {
                var date = getDocumentDateUtc(document);
                return date != DateTime.MinValue ? date : DefaultDocumentDateFunc(document);
            };
        else
            _getDocumentDateUtc = DefaultDocumentDateFunc;
    }

    private readonly Func<object, DateTime> DefaultDocumentDateFunc = (document) =>
    {
        return document switch
        {
            null => throw new ArgumentNullException(nameof(document)),
            // This is also called when trying to create the document id.
            IIdentity { Id: not null } identityDoc when ObjectId.TryParse(identityDoc.Id, out var objectId) && objectId.CreationTime != DateTime.MinValue => objectId.CreationTime,
            IHaveCreatedDate createdDoc when createdDoc.CreatedUtc != DateTime.MinValue => createdDoc.CreatedUtc,
            _ => throw new ArgumentException("Unable to get document date.", nameof(document)),
        };
    };

    /// <summary>
    /// This should never be negative or less than the index time period (day or a month)
    /// </summary>
    public TimeSpan? MaxIndexAge
    {
        get => _maxIndexAge;
        set
        {
            if (value.HasValue && value.Value <= TimeSpan.Zero)
                throw new ArgumentException($"{nameof(MaxIndexAge)} must be positive");

            _maxIndexAge = value;
        }
    }

    public bool DiscardExpiredIndexes { get; set; } = true;

    protected virtual DateTime GetIndexExpirationDate(DateTime utcDate)
    {
        return MaxIndexAge.HasValue && MaxIndexAge > TimeSpan.Zero ? utcDate.EndOfDay().SafeAdd(MaxIndexAge.Value) : DateTime.MaxValue;
    }

    public IReadOnlyCollection<IndexAliasAge> Aliases => _frozenAliases.Value;

    public void AddAlias(string name, TimeSpan? maxAge = null)
    {
        _aliases.Add(new IndexAliasAge
        {
            Name = name,
            MaxAge = maxAge ?? TimeSpan.MaxValue
        });
    }

    public override Task ConfigureAsync() => Task.CompletedTask;

    protected override async Task CreateAliasAsync(string index, string name)
    {
        await base.CreateAliasAsync(index, name).AnyContext();

        var utcDate = GetIndexDate(index);
        string alias = GetIndexByDate(utcDate);
        var indexExpirationUtcDate = GetIndexExpirationDate(utcDate);
        var expires = indexExpirationUtcDate < DateTime.MaxValue ? indexExpirationUtcDate : (DateTime?)null;
        await _aliasCache.SetAsync(alias, alias, expires).AnyContext();
    }

    protected string DateFormat { get; set; } = "yyyy.MM.dd";

    public string GetVersionedIndex(DateTime utcDate, int? version = null)
    {
        if (version == null || version < 0)
            version = Version;

        return $"{Name}-v{version}-{utcDate.ToString(DateFormat)}";
    }

    protected override DateTime GetIndexDate(string index)
    {
        int version = GetIndexVersion(index);
        if (version < 0)
            version = Version;

        if (DateTime.TryParseExact(index, $"\'{Name}-v{version}-\'{DateFormat}", EnUs, DateTimeStyles.AdjustToUniversal, out var result))
            return DateTime.SpecifyKind(result.Date, DateTimeKind.Utc);

        return DateTime.MaxValue;
    }

    protected async Task EnsureDateIndexAsync(DateTime utcDate)
    {
        utcDate = utcDate.Date;
        if (_ensuredDates.ContainsKey(utcDate))
            return;

        var indexExpirationUtcDate = GetIndexExpirationDate(utcDate);
        if (Configuration.TimeProvider.GetUtcNow().UtcDateTime > indexExpirationUtcDate)
            throw new ArgumentException($"Index max age exceeded: {indexExpirationUtcDate}", nameof(utcDate));

        var expires = indexExpirationUtcDate < DateTime.MaxValue ? indexExpirationUtcDate : (DateTime?)null;
        string unversionedIndexAlias = GetIndexByDate(utcDate);
        if (await _aliasCache.ExistsAsync(unversionedIndexAlias).AnyContext())
        {
            _ensuredDates[utcDate] = null;
            return;
        }

        if (await AliasExistsAsync(unversionedIndexAlias).AnyContext())
        {
            _ensuredDates[utcDate] = null;
            await _aliasCache.SetAsync(unversionedIndexAlias, unversionedIndexAlias, expires).AnyContext();
            return;
        }

        // try creating the index.
        string index = GetVersionedIndex(utcDate);
        await CreateIndexAsync(index, descriptor =>
        {
            ConfigureIndex(descriptor);
            // Add dated alias (e.g., daily-logevents-2025.12.01)
            descriptor.AddAlias(unversionedIndexAlias, a => { });
            // Add configured time-based aliases (includes base Name alias and any configured aliases like daily-logevents-today)
            foreach (var alias in Aliases.Where(a => ShouldCreateAlias(utcDate, a)))
                descriptor.AddAlias(alias.Name, a => { });
        }).AnyContext();

        _ensuredDates[utcDate] = null;
        await _aliasCache.SetAsync(unversionedIndexAlias, unversionedIndexAlias, expires).AnyContext();
    }

    protected virtual bool ShouldCreateAlias(DateTime documentDateUtc, IndexAliasAge alias)
    {
        if (alias.MaxAge == TimeSpan.MaxValue)
            return true;

        return Configuration.TimeProvider.GetUtcNow().UtcDateTime.Date.SafeSubtract(alias.MaxAge) <= documentDateUtc.EndOfDay();
    }

    public override async Task<int> GetCurrentVersionAsync()
    {
        var indexes = await GetIndexesAsync().AnyContext();
        if (indexes.Count == 0)
            return Version;

        var nonExpiredIndexes = indexes
            .Where(i => Configuration.TimeProvider.GetUtcNow().UtcDateTime <= GetIndexExpirationDate(i.DateUtc))
            .ToList();

        // First try to find indexes that have a valid dated alias (CurrentVersion >= 0)
        var indexesWithAlias = nonExpiredIndexes
            .Where(i => i.CurrentVersion >= 0)
            .Select(i => i.CurrentVersion)
            .OrderBy(v => v)
            .ToList();

        if (indexesWithAlias.Count > 0)
            return indexesWithAlias.First();

        // Fall back to oldest index version if no aliases exist
        var allVersions = nonExpiredIndexes
            .Select(i => i.Version)
            .OrderBy(v => v)
            .ToList();

        return allVersions.Count > 0 ? allVersions.First() : Version;
    }

    public virtual string[] GetIndexes(DateTime? utcStart, DateTime? utcEnd)
    {
        if (!utcStart.HasValue)
            utcStart = Configuration.TimeProvider.GetUtcNow().UtcDateTime;

        if (!utcEnd.HasValue || utcEnd.Value < utcStart)
            utcEnd = Configuration.TimeProvider.GetUtcNow().UtcDateTime;

        var utcStartOfDay = utcStart.Value.StartOfDay();
        var utcEndOfDay = utcEnd.Value.EndOfDay();
        var period = utcEndOfDay - utcStartOfDay;
        if ((MaxIndexAge.HasValue && period > MaxIndexAge.Value) || period.GetTotalMonths() >= 3)
            return Array.Empty<string>();

        // TODO: Look up aliases that fit these ranges.
        var indices = new List<string>();
        for (var current = utcStartOfDay; current <= utcEndOfDay; current = current.AddDays(1))
            indices.Add(GetIndexByDate(current));

        return indices.ToArray();
    }

    public override Task DeleteAsync()
    {
        return DeleteIndexAsync($"{Name}-v*");
    }

    public override async Task ReindexAsync(Func<int, string, Task> progressCallbackAsync = null)
    {
        int currentVersion = await GetCurrentVersionAsync().AnyContext();
        if (currentVersion < 0 || currentVersion >= Version)
            return;

        var indexes = await GetIndexesAsync(currentVersion).AnyContext();
        if (indexes.Count == 0)
            return;

        var reindexer = new ElasticReindexer(Configuration.Client, _logger);
        foreach (var index in indexes)
        {
            if (Configuration.TimeProvider.GetUtcNow().UtcDateTime > GetIndexExpirationDate(index.DateUtc))
                continue;

            if (index.CurrentVersion > Version)
                continue;

            var reindexWorkItem = new ReindexWorkItem
            {
                OldIndex = index.Index,
                NewIndex = GetVersionedIndex(GetIndexDate(index.Index), Version),
                Alias = Name,
                TimestampField = GetTimeStampField()
            };

            reindexWorkItem.DeleteOld = DiscardIndexesOnReindex && reindexWorkItem.OldIndex != reindexWorkItem.NewIndex;

            // attempt to create the index. If it exists the index will not be created.
            await CreateIndexAsync(reindexWorkItem.NewIndex, ConfigureIndex).AnyContext();

            // TODO: progress callback will report 0-100% multiple times...
            await reindexer.ReindexAsync(reindexWorkItem, progressCallbackAsync).AnyContext();
        }
    }

    public override async Task MaintainAsync(bool includeOptionalTasks = true)
    {
        var indexes = await GetIndexesAsync().AnyContext();
        if (indexes.Count == 0)
            return;

        await UpdateAliasesAsync(indexes).AnyContext();

        if (includeOptionalTasks && DiscardExpiredIndexes && MaxIndexAge.HasValue && MaxIndexAge > TimeSpan.Zero)
            await DeleteOldIndexesAsync(indexes).AnyContext();
    }

    protected virtual async Task UpdateAliasesAsync(IList<IndexInfo> indexes)
    {
        if (indexes.Count == 0)
            return;

        var aliasActions = new List<IndexUpdateAliasesAction>();

        foreach (var indexGroup in indexes.OrderBy(i => i.Version).GroupBy(i => i.DateUtc))
        {
            var indexExpirationDate = GetIndexExpirationDate(indexGroup.Key);

            // Ensure the current version is always set.
            if (Configuration.TimeProvider.GetUtcNow().UtcDateTime < indexExpirationDate)
            {
                var oldestIndex = indexGroup.First();
                if (oldestIndex.CurrentVersion < 0)
                {
                    try
                    {
                        await CreateAliasAsync(oldestIndex.Index, GetIndexByDate(indexGroup.Key)).AnyContext();
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error setting current index version. Will use oldest index version: {OldestIndexVersion}", oldestIndex.Version);
                    }

                    foreach (var indexInfo in indexGroup)
                        indexInfo.CurrentVersion = oldestIndex.Version;
                }
            }

            foreach (var index in indexGroup)
            {
                if (Configuration.TimeProvider.GetUtcNow().UtcDateTime >= indexExpirationDate || index.Version != index.CurrentVersion)
                {
                    foreach (var alias in Aliases)
                    {
                        aliasActions.Add(new IndexUpdateAliasesAction { Remove = new RemoveAction { Index = index.Index, Alias = alias.Name } });
                    }

                    continue;
                }

                foreach (var alias in Aliases)
                {
                    if (ShouldCreateAlias(indexGroup.Key, alias))
                        aliasActions.Add(new IndexUpdateAliasesAction { Add = new AddAction { Index = index.Index, Alias = alias.Name } });
                    else
                        aliasActions.Add(new IndexUpdateAliasesAction { Remove = new RemoveAction { Index = index.Index, Alias = alias.Name } });
                }
            }
        }

        if (aliasActions.Count == 0)
            return;

        var response = await Configuration.Client.Indices.UpdateAliasesAsync(u => u.Actions(aliasActions)).AnyContext();

        if (response.IsValidResponse)
        {
            _logger.LogRequest(response);
        }
        else
        {
            if (response.ApiCallDetails.HttpStatusCode.GetValueOrDefault() == 404)
                return;

            throw new DocumentException(response.GetErrorMessage("Error updating aliases"), response.OriginalException());
        }
    }

    protected virtual async Task DeleteOldIndexesAsync(IList<IndexInfo> indexes)
    {
        if (indexes.Count == 0 || !MaxIndexAge.HasValue || MaxIndexAge <= TimeSpan.Zero)
            return;

        var sw = new Stopwatch();
        foreach (var index in indexes.Where(i => Configuration.TimeProvider.GetUtcNow().UtcDateTime > GetIndexExpirationDate(i.DateUtc)))
        {
            sw.Restart();
            try
            {
                await DeleteIndexAsync(index.Index).AnyContext();
                _logger.LogInformation("Deleted index {Index} of age {Age:g} in {Duration:g}", index.Index,
                    Configuration.TimeProvider.GetUtcNow().UtcDateTime.Subtract(index.DateUtc), sw.Elapsed);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error deleting index {Index} of age {Age:g} in {Duration:g}", index.Index,
                    Configuration.TimeProvider.GetUtcNow().UtcDateTime.Subtract(index.DateUtc), sw.Elapsed);
            }

            sw.Stop();
        }
    }

    protected override async Task DeleteIndexAsync(string name)
    {
        await base.DeleteIndexAsync(name).AnyContext();

        if (name.EndsWith("*"))
        {
            await _aliasCache.RemoveAllAsync().AnyContext();
            _ensuredDates.Clear();
        }
        else
        {
            await _aliasCache.RemoveAsync(GetIndexByDate(GetIndexDate(name))).AnyContext();
        }
    }

    public override string[] GetIndexesByQuery(IRepositoryQuery query)
    {
        var indexes = GetIndexes(query);
        return indexes.Count > 0 ? indexes.ToArray() : _defaultIndexes;
    }

    private HashSet<string> GetIndexes(IRepositoryQuery query)
    {
        var indexes = new HashSet<string>();

        var elasticIndexes = query.GetElasticIndexes();
        if (elasticIndexes.Count > 0)
            indexes.AddRange(elasticIndexes);

        var utcStart = query.GetElasticIndexesStartUtc();
        var utcEnd = query.GetElasticIndexesEndUtc();
        if (utcStart.HasValue || utcEnd.HasValue)
            indexes.AddRange(GetIndexes(utcStart, utcEnd));

        return indexes;
    }

    protected override string GetIndexByDate(DateTime date)
    {
        return $"{Name}-{date.ToString(DateFormat)}";
    }

    protected override ElasticMappingResolver CreateMappingResolver()
    {
        return ElasticMappingResolver.Create(GetLatestIndexMapping, Configuration.Client.Infer, _logger);
    }

    protected TypeMapping GetLatestIndexMapping()
    {
        string filter = $"{Name}-v{Version}-*";
        var indicesResponse = Configuration.Client.Indices.Get((Indices)(IndexName)filter);
        if (!indicesResponse.IsValidResponse)
        {
            if (indicesResponse.ElasticsearchServerError?.Status == 404)
                return null;

            throw new RepositoryException(indicesResponse.GetErrorMessage($"Error getting latest index mapping {filter}"), indicesResponse.OriginalException());
        }

        var latestIndex = indicesResponse.Indices.Keys
            .Where(i => GetIndexVersion(i.ToString()) == Version)
            .Select(i =>
            {
                string indexName = i.ToString();
                return new IndexInfo { DateUtc = GetIndexDate(indexName), Index = indexName, Version = GetIndexVersion(indexName) };
            })
            .OrderByDescending(i => i.DateUtc)
            .FirstOrDefault();

        if (latestIndex == null)
            return null;

        var mappingResponse = Configuration.Client.Indices.GetMapping(new GetMappingRequest(latestIndex.Index));
        _logger.LogTrace("GetMapping: {Request}", mappingResponse.GetRequest(false, true));

        if (!mappingResponse.IsValidResponse)
        {
            _logger.LogError("Error getting mapping for {Index}: {Error}", latestIndex.Index, mappingResponse.ElasticsearchServerError);
            return null;
        }

        // use first returned mapping because index could have been an index alias
        var mapping = mappingResponse.Mappings.Values.FirstOrDefault()?.Mappings;
        return mapping;
    }

    public override string GetIndex(object target)
    {
        if (target == null)
            throw new ArgumentNullException(nameof(target));

        if (target is DateTimeOffset dto)
            return GetIndexByDate(dto.UtcDateTime);

        if (target is DateTime dt)
            return GetIndexByDate(dt);

        if (target is Id id)
        {
            if (!ObjectId.TryParse(id.Value, out var objectId))
                throw new ArgumentException("Unable to parse ObjectId", nameof(id));

            return GetIndexByDate(objectId.CreationTime);
        }

        if (target is ObjectId oid)
            return GetIndexByDate(oid.CreationTime);

        if (target is string { Length: 24 } stringId && ObjectId.TryParse(stringId, out var stringObjectId))
            return GetIndexByDate(stringObjectId.CreationTime);

        if (_getDocumentDateUtc == null)
            throw new ArgumentException("Unable to get document index", nameof(target));

        var date = _getDocumentDateUtc(target);
        return GetIndexByDate(date);
    }

    public override string CreateDocumentId(object document)
    {
        if (_getDocumentDateUtc != null)
        {
            var date = _getDocumentDateUtc(document);
            if (date != DateTime.MinValue)
                return ObjectId.GenerateNewId(date).ToString();
        }

        return base.CreateDocumentId(document);
    }

    public override Task EnsureIndexAsync(object target)
    {
        if (target == null)
            throw new ArgumentNullException(nameof(target));

        if (target is DateTime dt)
            return EnsureDateIndexAsync(dt);

        if (target is Id id)
        {
            if (!ObjectId.TryParse(id.Value, out var objectId))
                throw new ArgumentException("Unable to parse ObjectId", nameof(id));

            return EnsureDateIndexAsync(objectId.CreationTime);
        }

        if (target is ObjectId oid)
        {
            return EnsureDateIndexAsync(oid.CreationTime);
        }

        if (_getDocumentDateUtc == null)
            throw new ArgumentException("Unable to get document index", nameof(target));

        var date = _getDocumentDateUtc(target);
        return EnsureDateIndexAsync(date);
    }

    public override void Dispose()
    {
        _ensuredDates.Clear();
        base.Dispose();
    }

    [DebuggerDisplay("Name: {Name} Max Age: {MaxAge}")]
    public class IndexAliasAge
    {
        public string Name { get; set; }
        public TimeSpan MaxAge { get; set; }
    }
}

public class DailyIndex<T> : DailyIndex, IIndex<T> where T : class
{
    private readonly string _typeName = typeof(T).Name.ToLower();

    public DailyIndex(IElasticConfiguration configuration, string name = null, int version = 1, Func<object, DateTime> getDocumentDateUtc = null) : base(configuration, name, version, getDocumentDateUtc)
    {
        Name = name ?? _typeName;
    }

    protected override ElasticMappingResolver CreateMappingResolver()
    {
        return ElasticMappingResolver.Create<T>(ConfigureIndexMapping, Configuration.Client.Infer, GetLatestIndexMapping, _logger);
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
