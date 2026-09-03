using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Clients.Elasticsearch.Mapping;
using Foundatio.Lock;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Jobs;
using Foundatio.Repositories.Elasticsearch.Utility;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Models;
using Microsoft.Extensions.Logging;

namespace Foundatio.Repositories.Elasticsearch.Configuration;

public interface IVersionedIndex : IIndex
{
    int Version { get; }
    string VersionedName { get; }
    Task<int> GetCurrentVersionAsync();
    ReindexWorkItem CreateReindexWorkItem(int currentVersion);
}

public class VersionedIndex : Index, IVersionedIndex
{
    public VersionedIndex(IElasticConfiguration configuration, string name, int version = 1)
        : base(configuration, name)
    {
        Version = version;
        VersionedName = String.Concat(Name, "-v", Version);
    }

    public int Version { get; }
    public string VersionedName { get; }
    public bool DiscardIndexesOnReindex { get; set; } = true;
    private List<ReindexScript> ReindexScripts { get; } = new List<ReindexScript>();

    private record ReindexScript
    {
        public int Version { get; init; }
        public required string Script { get; init; }
    }

    protected virtual void AddReindexScript(int versionNumber, string script)
    {
        ReindexScripts.Add(new ReindexScript { Version = versionNumber, Script = script });
    }

    protected void RenameFieldScript(int versionNumber, string originalName, string currentName, bool removeOriginal = true)
    {
        PainlessFieldPath.Validate(originalName);
        PainlessFieldPath.Validate(currentName);

        if (String.Equals(originalName, currentName, StringComparison.Ordinal))
            throw new ArgumentException($"Original name '{originalName}' and current name cannot be the same.", nameof(currentName));

        string guard = BuildContainsKeyGuard(originalName);
        string accessor = BuildFieldAccessor(originalName);
        string assignment = BuildFieldAssignment(currentName, accessor);
        string script = $"if ({guard}) {{ {assignment} }}";
        ReindexScripts.Add(new ReindexScript { Version = versionNumber, Script = script });

        if (removeOriginal)
            RemoveFieldScript(versionNumber, originalName);
    }

    protected void RemoveFieldScript(int versionNumber, string fieldName)
    {
        PainlessFieldPath.Validate(fieldName);

        string guard = BuildContainsKeyGuard(fieldName);
        string removal = BuildFieldRemoval(fieldName);
        string script = $"if ({guard}) {{ {removal} }}";
        ReindexScripts.Add(new ReindexScript { Version = versionNumber, Script = script });
    }

    private static string BuildContainsKeyGuard(string fieldPath)
    {
        int dotIndex = fieldPath.LastIndexOf('.');
        if (dotIndex < 0)
            return $"ctx._source.containsKey('{fieldPath}')";

        string leaf = fieldPath[(dotIndex + 1)..];
        var segments = fieldPath[..dotIndex].Split('.');
        var sb = new StringBuilder();
        var prefix = "ctx._source";
        foreach (string segment in segments)
        {
            if (sb.Length > 0)
                sb.Append(" && ");
            string accessor = PainlessFieldPath.AppendSegment(prefix, segment);
            sb.Append(accessor).Append(" != null");
            prefix = accessor;
        }

        sb.Append(" && ").Append(prefix).Append(".containsKey('").Append(leaf).Append("')");
        return sb.ToString();
    }

    private static string BuildFieldAccessor(string fieldPath)
    {
        var segments = fieldPath.Split('.');
        var prefix = "ctx._source";
        foreach (string segment in segments)
            prefix = PainlessFieldPath.AppendSegment(prefix, segment);
        return prefix;
    }

    private static string BuildFieldAssignment(string targetPath, string valueExpression)
    {
        int dotIndex = targetPath.LastIndexOf('.');
        if (dotIndex < 0)
            return $"{PainlessFieldPath.AppendSegment("ctx._source", targetPath)} = {valueExpression};";

        var segments = targetPath[..dotIndex].Split('.');
        var sb = new StringBuilder();
        var prefix = "ctx._source";
        foreach (string segment in segments)
        {
            string accessor = PainlessFieldPath.AppendSegment(prefix, segment);
            sb.Append("if (").Append(accessor)
              .Append(" == null) { ").Append(accessor)
              .Append(" = [:]; } ");
            prefix = accessor;
        }

        string leaf = targetPath[(dotIndex + 1)..];
        sb.Append(PainlessFieldPath.AppendSegment(prefix, leaf)).Append(" = ").Append(valueExpression).Append(';');
        return sb.ToString();
    }

    private static string BuildFieldRemoval(string fieldPath)
    {
        int dotIndex = fieldPath.LastIndexOf('.');
        if (dotIndex < 0)
            return $"ctx._source.remove('{fieldPath}');";

        string leaf = fieldPath[(dotIndex + 1)..];
        var parentSegments = fieldPath[..dotIndex].Split('.');
        var prefix = "ctx._source";
        foreach (string segment in parentSegments)
            prefix = PainlessFieldPath.AppendSegment(prefix, segment);
        return $"{prefix}.remove('{leaf}');";
    }

    public override async Task ConfigureAsync()
    {
        if (!await IndexExistsAsync(VersionedName).AnyContext())
        {
            if (!await AliasExistsAsync(Name).AnyContext())
            {
                await CreateIndexAsync(VersionedName, d =>
                {
                    ConfigureIndex(d);
                    d.Aliases(ad => ad.Add(Name, a => { }));
                }).AnyContext();
            }
            else // new version of an existing index, don't set the alias yet
                await CreateIndexAsync(VersionedName, ConfigureIndex).AnyContext();
        }
        else
        {
            await UpdateIndexAsync(VersionedName).AnyContext();
        }
    }

    protected override ElasticMappingResolver CreateMappingResolver()
    {
        return ElasticMappingResolver.Create(Configuration.Client, VersionedName, _logger);
    }

    protected virtual async Task CreateAliasAsync(string index, string name)
    {
        if (await AliasExistsAsync(name).AnyContext())
            return;

        var response = await Configuration.Client.Indices.UpdateAliasesAsync(a => a.Actions(actions => actions.Add(s => s.Index(index).Alias(name)))).AnyContext();
        if (response.IsValidResponse)
        {
            _logger.LogRequest(response);
            return;
        }

        if (await AliasExistsAsync(name).AnyContext())
            return;

        _logger.LogErrorRequest(response, "Error creating alias {Name}", name);
        throw new RepositoryException(response.GetErrorMessage($"Error creating alias {name}"), response.OriginalException());
    }

    protected async Task<bool> AliasExistsAsync(string alias)
    {
        var response = await Configuration.Client.Indices.ExistsAliasAsync(Names.Parse(alias)).AnyContext();
        if (response.ApiCallDetails.HasSuccessfulStatusCode)
            return response.Exists;

        if (response.ApiCallDetails.HttpStatusCode is 404)
            return false;

        throw new RepositoryException(response.GetErrorMessage($"Error checking to see if alias {alias} exists"), response.OriginalException());
    }

    public override async Task DeleteAsync()
    {
        int currentVersion = await GetCurrentVersionAsync();
        var indexesToDelete = new List<string>(4);
        if (currentVersion != Version)
        {
            indexesToDelete.Add(String.Concat(Name, "-v", currentVersion));
            indexesToDelete.Add(String.Concat(Name, "-v", currentVersion, "-error"));
        }

        indexesToDelete.Add(VersionedName);
        indexesToDelete.Add(String.Concat(VersionedName, "-error"));
        await DeleteIndexesAsync(indexesToDelete.ToArray()).AnyContext();
    }

    public ReindexWorkItem CreateReindexWorkItem(int currentVersion)
    {
        var reindexWorkItem = new ReindexWorkItem
        {
            OldIndex = String.Concat(Name, "-v", currentVersion),
            NewIndex = VersionedName,
            Alias = Name,
            Script = GetReindexScripts(currentVersion),
            TimestampField = GetTimeStampField(),
            ReindexBatchSize = ReindexBatchSize,
            ReindexRequestsPerSecond = ReindexRequestsPerSecond
        };

        reindexWorkItem.DeleteOld = DiscardIndexesOnReindex && reindexWorkItem.OldIndex != reindexWorkItem.NewIndex;

        return reindexWorkItem;
    }

    protected string? GetReindexScripts(int currentVersion)
    {
        var scripts = ReindexScripts.Where(s => s.Version > currentVersion && Version >= s.Version).OrderBy(s => s.Version).ToList();
        if (scripts.Count == 0)
            return null;

        if (scripts.Count == 1)
            return scripts[0].Script;

        var sb = new StringBuilder();
        var calls = new StringBuilder();
        for (int i = 0; i < scripts.Count; i++)
        {
            sb.Append("void f").Append(i.ToString("000")).Append("(def ctx) { ").Append(scripts[i].Script).Append(" }\r\n");
            calls.Append('f').Append(i.ToString("000")).Append("(ctx); ");
        }

        sb.Append(calls);
        return sb.ToString();
    }

    public override async Task ReindexAsync(Func<int, string?, Task>? progressCallbackAsync = null)
    {
        int currentVersion = await GetCurrentVersionAsync().AnyContext();
        if (currentVersion < 0 || currentVersion >= Version)
            return;

        string lockKey = ElasticReindexer.GetLockName(Name);
        await using var reindexLock = await Configuration.LockProvider.AcquireAsync(lockKey, TimeSpan.FromMinutes(20), TimeSpan.FromMinutes(30)).AnyContext();

        currentVersion = await GetCurrentVersionAsync().AnyContext();
        if (currentVersion < 0 || currentVersion >= Version)
            return;

        var currentIndexes = await GetIndexesAsync(currentVersion).AnyContext();
        if (currentIndexes.Count is not 1)
            throw new RepositoryException($"Unable to identify a single physical index for schema version {currentVersion} of '{Name}'; found {currentIndexes.Count}.");
        var reindexWorkItem = CreateReindexWorkItem(currentVersion) with { OldIndex = currentIndexes[0].Index };

        Func<int, string?, Task> wrappedCallback = async (progress, message) =>
        {
            await reindexLock.RenewAsync().AnyContext();

            if (progressCallbackAsync is not null)
            {
                await progressCallbackAsync(progress, message).AnyContext();
            }
            else
            {
                _logger.LogInformation("Reindex Progress {Progress:F1}%: {Message}", progress, message);
            }
        };

        var reindexer = new ElasticReindexer(Configuration.Client, Configuration.Serializer, _logger);
        await reindexer.ReindexAsync(reindexWorkItem, wrappedCallback).AnyContext();
    }

    public override async Task MaintainAsync(bool includeOptionalTasks = true)
    {
        if (await AliasExistsAsync(Name).AnyContext())
            return;

        int currentVersion = await GetCurrentVersionAsync().AnyContext();
        if (currentVersion < 0)
            currentVersion = Version;

        var currentIndex = (await GetIndexesAsync(currentVersion).AnyContext()).FirstOrDefault();
        await CreateAliasAsync(currentIndex?.Index ?? String.Concat(Name, "-v", currentVersion), Name).AnyContext();
    }

    /// <summary>
    /// Returns the current index version (E.G., the oldest index version).
    /// </summary>
    /// <returns>-1 if there are no indexes.</returns>
    public virtual async Task<int> GetCurrentVersionAsync()
    {
        int version = await GetVersionFromAliasAsync(Name).AnyContext();
        if (version >= 0)
            return version;

        var indexes = await GetIndexesAsync().AnyContext();
        if (indexes.Count == 0)
            return Version;

        return indexes.Select(i => i.Version).OrderBy(v => v).First();
    }

    protected virtual async Task<int> GetVersionFromAliasAsync(string alias)
    {
        var response = await Configuration.Client.Indices.GetAliasAsync(a => a.Name(Names.Parse(alias))).AnyContext();
        if (!response.IsValidResponse && response.ElasticsearchServerError?.Status == 404)
            return -1;

#if ELASTICSEARCH9
        var indices = response.Aliases;
#else
        var indices = response.Values;
#endif
        if (response.IsValidResponse && indices != null && indices.Count > 0)
        {
            _logger.LogRequest(response);
            return indices.Keys.Select(i => GetIndexVersion(i.ToString())).OrderBy(v => v).First();
        }

        _logger.LogErrorRequest(response, "Error getting index version from alias");
        return -1;
    }

    protected virtual int GetIndexVersion(string name)
    {
        if (String.IsNullOrEmpty(name))
            throw new ArgumentNullException(nameof(name));

        ReadOnlySpan<char> canonicalName = CompatibilityIndexName.GetCanonicalNameSpan(name, Name);
        int versionStart = Name.Length + 2;
        if (canonicalName.Length <= versionStart
            || !canonicalName.StartsWith(Name.AsSpan(), StringComparison.Ordinal)
            || canonicalName[Name.Length] is not '-'
            || canonicalName[Name.Length + 1] is not 'v')
        {
            return -1;
        }

        int versionEnd = versionStart;
        while (versionEnd < canonicalName.Length && canonicalName[versionEnd] is >= '0' and <= '9')
            versionEnd++;

        if (versionEnd == versionStart
            || (versionEnd < canonicalName.Length && canonicalName[versionEnd] is not '-'))
        {
            return -1;
        }

        return Int32.TryParse(canonicalName[versionStart..versionEnd], out int version) ? version : -1;
    }

    protected override string GetCompatibilityIndexPattern()
    {
        return $"{Name}-v*";
    }

    internal override bool IsNativeIndexName(ReadOnlySpan<char> sourceIndex)
    {
        ReadOnlySpan<char> name = Name;
        if (sourceIndex.Length <= name.Length + 2
            || !sourceIndex.StartsWith(name, StringComparison.Ordinal)
            || sourceIndex[name.Length] is not '-'
            || sourceIndex[name.Length + 1] is not 'v')
        {
            return false;
        }

        int offset = name.Length + 2;
        int versionStart = offset;
        while (offset < sourceIndex.Length && sourceIndex[offset] is >= '0' and <= '9')
            offset++;

        if (offset == versionStart)
            return false;

        return HasMultipleIndexes || offset == sourceIndex.Length;
    }

    internal override void ValidateCompatibilityUpgradeSource(string sourceIndex, IReadOnlyDictionary<string, Alias>? aliases)
    {
        base.ValidateCompatibilityUpgradeSource(sourceIndex, aliases);
        int sourceVersion = GetIndexVersion(sourceIndex);
        if (sourceVersion != Version && aliases?.ContainsKey(Name) is true)
            throw new RepositoryException($"Index '{sourceIndex}' uses schema version {sourceVersion}, but '{Name}' is configured for version {Version}. Run the schema reindex before upgrading Elasticsearch index compatibility.");
    }

    protected virtual async Task<IList<IndexInfo>> GetIndexesAsync(int version = -1)
    {
        string filter = version < 0 ? $"{Name}-v*" : $"{Name}-v{version}";
        if (HasMultipleIndexes)
            filter += "-*";

        var sw = Stopwatch.StartNew();
        var response = await Configuration.Client.Indices.GetAsync((Indices)(IndexName)filter, d => d.LimitToNamesAndAliases().ExpandWildcards(ExpandWildcard.All).IgnoreUnavailable()).AnyContext();
        sw.Stop();
        _logger.LogRequest(response);

        if (!response.IsValidResponse)
        {
            if (response.ElasticsearchServerError?.Status == 404)
                return new List<IndexInfo>();

            throw new RepositoryException(response.GetErrorMessage($"Error getting indices {filter}"), response.OriginalException());
        }

        if (response.Indices.Count == 0)
            return new List<IndexInfo>();

        var indices = new List<IndexInfo>(response.Indices.Count);
        foreach (var entry in response.Indices)
        {
            string indexName = entry.Key;
            if (!IsDiscoveryCandidate(indexName, entry.Value))
                continue;

            int indexVersion = GetIndexVersion(indexName);
            if (indexVersion < 0 || (version >= 0 && indexVersion != version))
                continue;

            var indexDate = GetIndexDate(indexName);
            if (HasMultipleIndexes && indexDate == DateTime.MaxValue)
                continue;

            string indexAliasName = GetIndexByDate(indexDate);
            int currentVersion = entry.Value.Aliases?.ContainsKey(indexAliasName) is true ? indexVersion : -1;
            indices.Add(new IndexInfo { DateUtc = indexDate, Index = indexName, Version = indexVersion, CurrentVersion = currentVersion });
        }

        indices = indices.OrderBy(i => i.DateUtc).ToList();

        _logger.LogInformation("Retrieved list of {IndexCount} indexes in {Duration:g}", indices.Count, sw.Elapsed);
        return indices;
    }

    private protected bool IsDiscoveryCandidate(string indexName, IndexState? state)
    {
        if (state is null || state.Aliases.HasExactHiddenAlias(ElasticReindexer.ErrorIndexOwnershipAlias))
            return false;

        // Native names use the existing virtual date/version parsers; only wrappers need ownership checks.
        ReadOnlySpan<char> name = indexName;
        return (name.StartsWith(Name.AsSpan(), StringComparison.Ordinal) && name[Name.Length..].StartsWith("-v", StringComparison.Ordinal))
            || MatchesCompatibilitySource(indexName, state.Aliases);
    }

    protected virtual DateTime GetIndexDate(string name)
    {
        return DateTime.MaxValue;
    }

    protected virtual string GetIndexByDate(DateTime date)
    {
        return Name;
    }

    [DebuggerDisplay("{Index} (Date: {DateUtc} Version: {Version} CurrentVersion: {CurrentVersion})")]
    protected record IndexInfo
    {
        public required string Index { get; init; }
        public int Version { get; init; }
        public int CurrentVersion { get; set; } = -1;
        public DateTime DateUtc { get; init; }
    }
}

public class VersionedIndex<T> : VersionedIndex, IIndex<T> where T : class
{
    private static readonly string _typeName = typeof(T).Name.ToLower();

    public VersionedIndex(IElasticConfiguration configuration, string? name = null, int version = 1) : base(configuration, name ?? _typeName, version)
    {
    }

    protected override ElasticMappingResolver CreateMappingResolver()
    {
        return ElasticMappingResolver.Create<T>(ConfigureIndexMapping, Configuration.Client, VersionedName, _logger);
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
            m.Indices(name);
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

        // TODO: Check for issues with attempting to change existing fields and warn that index version needs to be incremented
        if (response.IsValidResponse)
            _logger.LogRequest(response);
        else
            _logger.LogErrorRequest(response, $"Error updating index ({name}) mappings. Changing existing fields requires a new index version.");
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
