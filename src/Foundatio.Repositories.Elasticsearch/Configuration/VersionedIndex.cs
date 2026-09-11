using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading;
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
    /// <summary>
    /// How long alias maintenance holds the reindex lock. Maintenance only rewrites aliases, so this bounds a
    /// crashed run's blast radius rather than covering a long operation, and it is deliberately not renewed.
    /// </summary>
    protected static readonly TimeSpan MaintenanceLockDuration = TimeSpan.FromMinutes(1);

    /// <summary>
    /// How long alias maintenance waits for the reindex lock before skipping the run. Kept short on purpose: a
    /// reindex holds this lock for minutes to hours, so waiting longer would never turn a skip into an
    /// acquisition — it would only stall callers like <c>ConfigureIndexesAsync</c>. The wait exists to ride out
    /// a competing maintenance run, which only issues a few alias calls.
    /// </summary>
    protected static readonly TimeSpan MaintenanceLockAcquireTimeout = TimeSpan.FromSeconds(5);

    /// <summary>
    /// How long a reindex holds its lock. Renewed on every progress report, so a migration that runs longer
    /// than this keeps the lock as long as it is making progress, while a crashed run releases it after this
    /// much silence rather than blocking migrations forever.
    /// </summary>
    protected static readonly TimeSpan ReindexLockDuration = TimeSpan.FromMinutes(20);

    /// <summary>
    /// How long a reindex waits for the lock before skipping. Generous because the holder is doing the same
    /// work this caller wants done: waiting lets a queued instance pick up where a finishing one left off,
    /// and the post-acquire version re-check turns an already-completed migration into a clean no-op.
    /// </summary>
    protected static readonly TimeSpan ReindexLockAcquireTimeout = TimeSpan.FromMinutes(30);

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
        var indexesToDelete = new List<string>();
        if (currentVersion != Version)
        {
            indexesToDelete.Add(String.Concat(Name, "-v", currentVersion));
            indexesToDelete.Add(ElasticReindexer.GetFailureIndexName(String.Concat(Name, "-v", currentVersion)));
        }

        indexesToDelete.Add(VersionedName);
        indexesToDelete.Add(ElasticReindexer.GetFailureIndexName(VersionedName));
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

    public override async Task ReindexAsync(Func<int, string?, Task>? progressCallbackAsync = null, CancellationToken cancellationToken = default)
    {
        await using var lease = await TryAcquireReindexLeaseAsync(cancellationToken).AnyContext();
        if (lease is null)
            return;

        var reindexWorkItem = CreateReindexWorkItem(lease.CurrentVersion);

        var reindexer = new ElasticReindexer(Configuration.Client, Configuration.Serializer, Configuration.TimeProvider, Configuration.ResiliencePolicyProvider, _logger);
        await reindexer.ReindexAsync(reindexWorkItem, CreateReindexProgressCallback(lease.Lock, progressCallbackAsync), cancellationToken).AnyContext();
    }

    /// <summary>
    /// Holds the reindex lock together with the index version observed after the lock was taken.
    /// </summary>
    protected sealed class ReindexLease : IAsyncDisposable
    {
        public ReindexLease(ILock reindexLock, int currentVersion)
        {
            Lock = reindexLock;
            CurrentVersion = currentVersion;
        }

        public ILock Lock { get; }
        public int CurrentVersion { get; }

        public ValueTask DisposeAsync() => Lock.DisposeAsync();
    }

    /// <summary>
    /// Acquires the reindex lock and re-reads the version under it, returning <c>null</c> when there is
    /// nothing to do: the index is already current, or another migration holds the lock.
    /// </summary>
    /// <remarks>
    /// The version is deliberately read twice. The first read avoids taking the lock at all in the common
    /// no-op case; the second is the authoritative one, because another process may have completed the
    /// migration while this one waited for the lock.
    /// </remarks>
    /// <param name="cancellationToken">
    /// Cancels the wait for the lock. Without this a cancelled reindex would still block for the full acquire
    /// timeout before the token was ever observed.
    /// </param>
    protected async Task<ReindexLease?> TryAcquireReindexLeaseAsync(CancellationToken cancellationToken = default)
    {
        int currentVersion = await GetCurrentVersionAsync().AnyContext();
        if (currentVersion < 0 || currentVersion >= Version)
            return null;

        string lockKey = ElasticReindexer.GetLockName(Name);
        ILock? reindexLock;

        // There is no acquire overload taking both a timeout and a token, so the timeout is expressed as a
        // linked token - the same approach ReindexWorkItemHandler uses. Cancellation and timeout then arrive
        // as the same signal, and the two are distinguished below by inspecting the caller's token.
        using var acquireTimeoutSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        acquireTimeoutSource.CancelAfter(ReindexLockAcquireTimeout);

        try
        {
            reindexLock = await Configuration.LockProvider.AcquireAsync(lockKey, ReindexLockDuration, acquireTimeoutSource.Token).AnyContext();
        }
        catch (LockAcquisitionTimeoutException)
        {
            // How the real providers report contention. Losing the race is the lock working, not a failure:
            // the holder is migrating this index, so this caller has nothing to do. Left to propagate it would
            // surface as a failed migration - and ElasticConfiguration.ReindexAsync now aggregates and throws,
            // so two instances starting together would fail startup on whichever one lost.
            _logger.LogInformation("Skipping reindex of {Index}: lock {LockKey} is held, so another migration is in progress.", Name, lockKey);
            return null;
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            // The acquire timeout elapsed rather than the caller cancelling, which is contention again.
            _logger.LogInformation("Skipping reindex of {Index}: lock {LockKey} could not be acquired within {AcquireTimeout:g}, so another migration is in progress.", Name, lockKey, ReindexLockAcquireTimeout);
            return null;
        }

        // AcquireAsync is declared as returning a non-nullable ILock, but the interface does not forbid null
        // and an implementation may return it instead of throwing. Both denials must degrade to a clean skip,
        // never to a NullReferenceException at the first RenewAsync.
        if (reindexLock is null)
        {
            _logger.LogWarning("Skipping reindex of {Index}: could not acquire lock {LockKey} within the timeout. Another migration is likely in progress.", Name, lockKey);
            return null;
        }

        currentVersion = await GetCurrentVersionAsync().AnyContext();
        if (currentVersion < 0 || currentVersion >= Version)
        {
            await reindexLock.DisposeAsync().AnyContext();
            return null;
        }

        return new ReindexLease(reindexLock, currentVersion);
    }

    /// <summary>
    /// Wraps the caller's progress callback so the reindex lock is renewed on every progress report, which
    /// is what keeps a migration longer than the lock's TTL from having the lock expire underneath it.
    /// </summary>
    protected Func<int, string?, Task> CreateReindexProgressCallback(ILock reindexLock, Func<int, string?, Task>? progressCallbackAsync)
    {
        return async (progress, message) =>
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
    }

    /// <summary>
    /// Acquires the <c>reindex:{alias}</c> lock for the duration of an alias maintenance run, or returns
    /// <c>null</c> when a reindex holds it and this run should be skipped.
    /// </summary>
    /// <remarks>
    /// Alias maintenance and a reindex cutover both rewrite the same aliases, so they share a lock. Callers
    /// must hold it only across reading the current alias state and writing the new one: the lock is not
    /// renewed, so anything unbounded (like deleting expired indexes) belongs outside it. A run that cannot
    /// get the lock is skipped rather than queued, because maintenance is periodic and idempotent.
    /// </remarks>
    protected async Task<ILock?> TryAcquireMaintenanceLockAsync()
    {
        string lockName = ElasticReindexer.GetLockName(Name);
        var maintenanceLock = await Configuration.LockProvider.TryAcquireAsync(lockName, MaintenanceLockDuration, MaintenanceLockAcquireTimeout).AnyContext();
        if (maintenanceLock is null)
            _logger.LogInformation("Skipping alias maintenance of {Index}: could not acquire lock {LockName} within {AcquireTimeout:g}. A reindex is likely in progress.", Name, lockName, MaintenanceLockAcquireTimeout);

        return maintenanceLock;
    }

    /// <summary>
    /// Restores the alias when it is missing, which is how an index whose alias was lost gets back into
    /// queries.
    /// </summary>
    /// <remarks>
    /// The alias is checked twice. The first check keeps the overwhelmingly common case (the alias is
    /// present) off the lock entirely; the second is the authoritative one, because a reindex cutover may
    /// have created the alias while this call waited for the lock. Without the second check this would add
    /// the alias back onto the <em>old</em> version, leaving the alias pointing at two versions at once.
    /// </remarks>
    public override async Task MaintainAsync(bool includeOptionalTasks = true)
    {
        if (await AliasExistsAsync(Name).AnyContext())
            return;

        await using var maintenanceLock = await TryAcquireMaintenanceLockAsync().AnyContext();
        if (maintenanceLock is null)
            return;

        if (await AliasExistsAsync(Name).AnyContext())
            return;

        int currentVersion = await GetCurrentVersionAsync().AnyContext();
        if (currentVersion < 0)
            currentVersion = Version;

        await CreateAliasAsync(String.Concat(Name, "-v", currentVersion), Name).AnyContext();
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

        string namePrefix = $"{Name}-v";
        if (name.Length <= namePrefix.Length || !name.StartsWith(namePrefix))
            return -1;

        string input = name.Substring($"{Name}-v".Length);
        int index = input.IndexOf('-');
        if (index > 0)
            input = input.Substring(0, index);

        if (Int32.TryParse(input, out int version))
            return version;

        return -1;
    }

    protected virtual async Task<IList<IndexInfo>> GetIndexesAsync(int version = -1)
    {
        string filter = version < 0 ? $"{Name}-v*" : $"{Name}-v{version}";
        if (HasMultipleIndexes)
            filter += "-*";

        var sw = Stopwatch.StartNew();
        var response = await Configuration.Client.Indices.GetAsync((Indices)(IndexName)filter, d => d.LimitToNamesAndAliases()).AnyContext();
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

        var aliasResponse = await Configuration.Client.Indices.GetAliasAsync(a => a.Name($"{Name}-*")).AnyContext();
        _logger.LogRequest(aliasResponse);

        if (!aliasResponse.IsValidResponse && aliasResponse.ElasticsearchServerError?.Status != 404)
            throw new RepositoryException(aliasResponse.GetErrorMessage($"Error getting index aliases for {filter}"), aliasResponse.OriginalException());

#if ELASTICSEARCH9
        var aliasIndices = aliasResponse.Aliases;
#else
        var aliasIndices = aliasResponse.Values;
#endif
        var indices = response.Indices.Keys
            .Where(i => version < 0 || GetIndexVersion(i.ToString()) == version)
            .Select(i =>
            {
                string indexName = i.ToString();
                var indexDate = GetIndexDate(indexName);
                string indexAliasName = GetIndexByDate(GetIndexDate(indexName));

                int currentVersion = -1;
                if (aliasResponse.IsValidResponse && aliasIndices != null && aliasIndices.TryGetValue(i, out var indexAliases))
                {
                    // Find if any of our aliases point to this index
                    if (indexAliases.Aliases.ContainsKey(indexAliasName))
                        currentVersion = GetIndexVersion(indexName);
                }

                return new IndexInfo { DateUtc = indexDate, Index = indexName, Version = GetIndexVersion(indexName), CurrentVersion = currentVersion };
            })
            .OrderBy(i => i.DateUtc)
            .ToList();

        _logger.LogInformation("Retrieved list of {IndexCount} indexes in {Duration:g}", indices.Count, sw.Elapsed);
        return indices;
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
