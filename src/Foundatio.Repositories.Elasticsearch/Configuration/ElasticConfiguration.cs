using System;
using System.Collections.Generic;
using System.IO.Hashing;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Serialization;
using Elastic.Transport;
using Foundatio.Caching;
using Foundatio.Jobs;
using Foundatio.Lock;
using Foundatio.Messaging;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Queues;
using Foundatio.Repositories.Elasticsearch.CustomFields;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Jobs;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Serialization;
using Foundatio.Resilience;
using Foundatio.Serializer;
using Foundatio.Utility;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Foundatio.Repositories.Elasticsearch.Configuration;

public class ElasticConfiguration : IElasticConfigurationCompatibility
{
    protected readonly IQueue<WorkItemData>? _workItemQueue;
    protected readonly ILogger _logger;
    protected readonly ILockProvider _beginReindexLockProvider;
    protected readonly ILockProvider _lockProvider;
    private readonly List<IIndex> _indexes = new();
    private readonly Lazy<IReadOnlyCollection<IIndex>> _frozenIndexes;
    private readonly Lazy<ElasticsearchClient> _client;
    private readonly Lazy<ICustomFieldDefinitionRepository?> _customFieldDefinitionRepository;
    protected readonly bool _shouldDisposeCache;
    private readonly bool _shouldDisposeMessageBus;
    private readonly ICacheClient _configureIndexesCache;
    public const string ConfigureIndexesResourceName = "configure-indexes";
    private int _disposed;

    public ElasticConfiguration(IQueue<WorkItemData>? workItemQueue = null, ICacheClient? cacheClient = null, IMessageBus? messageBus = null, ITextSerializer? serializer = null, TimeProvider? timeProvider = null, IResiliencePolicyProvider? resiliencePolicyProvider = null, ILoggerFactory? loggerFactory = null)
    {
        _workItemQueue = workItemQueue;
        TimeProvider = timeProvider ?? TimeProvider.System;
        LoggerFactory = loggerFactory ?? NullLoggerFactory.Instance;
        _logger = LoggerFactory.CreateLogger(GetType());

        if (serializer is null)
        {
            _logger.LogWarning("No serializer configured, using default System.Text.Json serializer");
            serializer = new Foundatio.Serializer.SystemTextJsonSerializer(
                new System.Text.Json.JsonSerializerOptions().ConfigureFoundatioRepositoryDefaults());
        }

        Serializer = serializer;
        ResiliencePolicyProvider = resiliencePolicyProvider ?? cacheClient?.GetResiliencePolicyProvider() ?? new ResiliencePolicyProvider();
        ResiliencePolicy = ResiliencePolicyProvider.GetPolicy<ElasticConfiguration>(_logger, TimeProvider);
        Cache = cacheClient ?? new InMemoryCacheClient(new InMemoryCacheClientOptions { CloneValues = true, ResiliencePolicyProvider = ResiliencePolicyProvider, TimeProvider = TimeProvider, LoggerFactory = LoggerFactory });
        _shouldDisposeCache = cacheClient is null;
        _configureIndexesCache = new ScopedCacheClient(Cache, ConfigureIndexesResourceName);
        _shouldDisposeMessageBus = messageBus is null;
        messageBus ??= new InMemoryMessageBus(new InMemoryMessageBusOptions { ResiliencePolicyProvider = ResiliencePolicyProvider, TimeProvider = TimeProvider, LoggerFactory = LoggerFactory });
        MessageBus = messageBus;
        _lockProvider = new CacheLockProvider(Cache, messageBus, TimeProvider, ResiliencePolicyProvider, LoggerFactory);
        _beginReindexLockProvider = new ThrottlingLockProvider(Cache, 1, TimeSpan.FromMinutes(15), TimeProvider, ResiliencePolicyProvider, LoggerFactory);
        _frozenIndexes = new Lazy<IReadOnlyCollection<IIndex>>(() => _indexes.AsReadOnly());
        _customFieldDefinitionRepository = new Lazy<ICustomFieldDefinitionRepository?>(CreateCustomFieldDefinitionRepository);
        _client = new Lazy<ElasticsearchClient>(CreateElasticClient);
    }

    protected virtual ElasticsearchClient CreateElasticClient()
    {
        var settings = new ElasticsearchClientSettings(
            CreateConnectionPool() ?? new SingleNodePool(new Uri("http://localhost:9200")),
            sourceSerializer: (_, clientSettings) =>
                new DefaultSourceSerializer(clientSettings, options => options.ConfigureFoundatioRepositoryDefaults()));
        ConfigureSettings(settings);
        foreach (var index in Indexes)
            index.ConfigureSettings(settings);

        return new ElasticsearchClient(settings);
    }

    public virtual void ConfigureGlobalQueryBuilders(ElasticQueryBuilder builder) { }

    public virtual void ConfigureGlobalQueryParsers(ElasticQueryParserConfiguration config) { }

    protected virtual void ConfigureSettings(ElasticsearchClientSettings settings)
    {
        settings.EnableTcpKeepAlive(TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(2));
    }

    protected virtual NodePool? CreateConnectionPool()
    {
        return null;
    }

    public ElasticsearchClient Client => _client.Value;
    public ICacheClient Cache { get; }
    public IMessageBus MessageBus { get; }
    public ITextSerializer Serializer { get; }
    public ILockProvider LockProvider => _lockProvider;
    public ILoggerFactory LoggerFactory { get; }
    public IResiliencePolicyProvider ResiliencePolicyProvider { get; }
    public IResiliencePolicy ResiliencePolicy { get; }
    public TimeProvider TimeProvider { get; set; }
    public IReadOnlyCollection<IIndex> Indexes => _frozenIndexes.Value;
    public ICustomFieldDefinitionRepository? CustomFieldDefinitionRepository => _customFieldDefinitionRepository.Value;

    private CustomFieldDefinitionIndex? _customFieldDefinitionIndex = null;
    private ICustomFieldDefinitionRepository? CreateCustomFieldDefinitionRepository()
    {
        if (_customFieldDefinitionIndex == null)
            return null;

        return new CustomFieldDefinitionRepository(_customFieldDefinitionIndex, _lockProvider);
    }

    public CustomFieldDefinitionIndex AddCustomFieldIndex(string name = "customfield", int replicas = 1)
    {
        _customFieldDefinitionIndex = new CustomFieldDefinitionIndex(this, name, replicas);
        AddIndex(_customFieldDefinitionIndex);
        return _customFieldDefinitionIndex;
    }

    public void AddIndex(IIndex index)
    {
        if (_frozenIndexes.IsValueCreated)
            throw new InvalidOperationException("Can't add indexes after the list has been frozen.");

        if (_indexes.Any(i => String.Equals(i.Name, index.Name, StringComparison.OrdinalIgnoreCase)))
            throw new ArgumentException($"An index with name '{index.Name}' has already been registered.", nameof(index));

        _indexes.Add(index);
    }

    public IIndex? GetIndex(string name)
    {
        foreach (var index in Indexes)
            if (index.Name.Equals(name, StringComparison.OrdinalIgnoreCase))
                return index;

        return null;
    }

    public async Task ConfigureIndexesAsync(IEnumerable<IIndex>? indexes = null, bool beginReindexingOutdated = true)
    {
        if (indexes is not null)
        {
            var indexList = indexes as ICollection<IIndex> ?? indexes.ToArray();
            _logger.LogInformation("Configuring {IndexCount} explicit indexes (beginReindexingOutdated={BeginReindexingOutdated})", indexList.Count, beginReindexingOutdated);
            await Task.WhenAll(indexList.Select(idx => ConfigureIndexInternalAsync(idx, beginReindexingOutdated))).AnyContext();
            return;
        }

        string cacheKey = GetConfigureIndexesCacheKey();

        if (await TryCheckCacheMarkerAsync(cacheKey).AnyContext())
        {
            _logger.LogInformation("Skipping index configuration, already configured recently");
            return;
        }

        await using var configLock = await _lockProvider.AcquireAsync(
            ConfigureIndexesResourceName, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(1)).AnyContext();

        if (configLock is null)
        {
            _logger.LogInformation("Skipping index configuration, another process is currently configuring");
            return;
        }

        if (await TryCheckCacheMarkerAsync(cacheKey).AnyContext())
        {
            _logger.LogInformation("Skipping index configuration, configured by another process while waiting for lock");
            return;
        }

        _logger.LogInformation("Configuring {IndexCount} indexes (beginReindexingOutdated={BeginReindexingOutdated})...", Indexes.Count, beginReindexingOutdated);

        await Task.WhenAll(Indexes.Select(idx => ConfigureIndexInternalAsync(idx, beginReindexingOutdated))).AnyContext();

        await TrySetCacheMarkerAsync(cacheKey).AnyContext();
    }

    private async Task ConfigureIndexInternalAsync(IIndex idx, bool beginReindexingOutdated)
    {
        await idx.ConfigureAsync().AnyContext();
        await idx.MaintainAsync(includeOptionalTasks: false).AnyContext();

        if (!beginReindexingOutdated)
            return;

        if (idx is not IVersionedIndex versionedIndex)
            return;

        int currentVersion = await versionedIndex.GetCurrentVersionAsync().AnyContext();
        if (versionedIndex.Version <= currentVersion)
            return;

        if (_workItemQueue == null || _beginReindexLockProvider == null)
            throw new InvalidOperationException("Must specify work item queue and lock provider in order to migrate index versions.");

        var reindexWorkItem = versionedIndex.CreateReindexWorkItem(currentVersion);
        bool isReindexing = await _lockProvider.IsLockedAsync(ElasticReindexer.GetLockName(versionedIndex.Name)).AnyContext();
        if (isReindexing)
            return;

        // enqueue reindex to new version, only allowed every 15 minutes
        string enqueueReindexLockName = String.Join(":", "enqueue-reindex", reindexWorkItem.Alias, reindexWorkItem.OldIndex, reindexWorkItem.NewIndex);
        await _beginReindexLockProvider.TryUsingAsync(enqueueReindexLockName, async () => { await _workItemQueue.EnqueueAsync(reindexWorkItem).AnyContext(); }, TimeSpan.Zero, new CancellationToken(true)).AnyContext();
    }

    private static async Task<IReadOnlyCollection<ReindexWorkItem>> GetCompatibilityWorkItemsAsync(Index idx)
    {
        var infos = await idx.GetIndexCompatibilityAsync().AnyContext();
        return CreateCompatibilityReindexWorkItems(idx, infos);
    }

    internal static IReadOnlyCollection<ReindexWorkItem> CreateCompatibilityReindexWorkItems(Index idx, IReadOnlyCollection<IndexCompatibilityInfo> infos)
    {
        ArgumentNullException.ThrowIfNull(idx);
        ArgumentNullException.ThrowIfNull(infos);

        var revisions = infos.Where(i => i.RequiresReindexBeforeNextMajorUpgrade).Select(info =>
        {
            string originalName = idx is IVersionedIndex versionedIndex ? versionedIndex.VersionedName : idx.Name;
            bool isOriginalName = !idx.HasMultipleIndexes && String.Equals(info.Name, originalName, StringComparison.Ordinal);
            var revision = isOriginalName ? new IndexNameRevision(info.Name, 0, false) : IndexNameRevision.Parse(info.Name);

            return (Info: info, Name: revision);
        }).OrderBy(x => x.Name.BaseName, StringComparer.Ordinal).ThenBy(x => x.Name.Revision);

        var workItems = new List<ReindexWorkItem>();
        foreach (var revision in revisions)
        {
            if (revision.Name.Revision is Int32.MaxValue)
                throw new RepositoryException($"Cannot create another compatibility revision for index '{revision.Info.Name}'.");

            workItems.Add(new ReindexWorkItem
            {
                OldIndex = revision.Info.Name,
                NewIndex = $"{revision.Name.BaseName}-r{revision.Name.Revision + 1}",
                Alias = idx.Name,
                PreserveSourceIndexName = !revision.Name.HasRevision && !idx.HasMultipleIndexes,
                DeleteOld = true,
                TimestampField = idx.CompatibilityTimestampField,
                ReindexBatchSize = idx.ReindexBatchSize,
                ReindexRequestsPerSecond = idx.ReindexRequestsPerSecond
            });
        }

        return workItems;
    }

    private async Task EnsureCompatibilityDestinationsAvailableAsync(IReadOnlyCollection<ReindexWorkItem> workItems)
    {
        var existing = await GetConcreteIndexNamesAsync(workItems.Select(w => w.NewIndex)).AnyContext();
        if (existing.Count is 0)
            return;

        throw new RepositoryException($"Compatibility reindex destination indexes already exist: {String.Join(", ", existing)}. Inspect and remove or rename them before retrying.");
    }

    private async Task EnsureCompatibilitySourcesRemovedAsync(IReadOnlyCollection<ReindexWorkItem> workItems)
    {
        var sourceNames = workItems.Select(w => w.OldIndex).ToHashSet(StringComparer.Ordinal);
        var concreteNames = await GetConcreteIndexNamesAsync(sourceNames).AnyContext();
        var remainingSources = concreteNames.Where(sourceNames.Contains).ToArray();
        if (remainingSources.Length > 0)
            throw new RepositoryException($"Compatibility reindex did not remove source indexes: {String.Join(", ", remainingSources)}.");
    }

    private async Task<IReadOnlyCollection<string>> GetConcreteIndexNamesAsync(IEnumerable<string> names)
    {
        string joinedNames = String.Join(",", names.Distinct(StringComparer.Ordinal));
        var response = await Client.Indices.GetAsync(Indices.Parse(joinedNames), d => d.LimitToNamesAndAliases().IgnoreUnavailable()).AnyContext();
        if (!response.IsValidResponse && response.ElasticsearchServerError?.Status is not 404)
            throw new RepositoryException(response.GetErrorMessage($"Error resolving compatibility indexes {joinedNames}"), response.OriginalException());

        _logger.LogRequest(response);
        return response.Indices?.Keys.Select(k => k.ToString()).ToArray() ?? [];
    }

    public Task MaintainIndexesAsync(IEnumerable<IIndex>? indexes = null)
    {
        if (indexes is null)
            indexes = Indexes;

        var tasks = new List<Task>();
        foreach (var idx in indexes)
            tasks.Add(idx.MaintainAsync());

        return Task.WhenAll(tasks);
    }

    public async Task DeleteIndexesAsync(IEnumerable<IIndex>? indexes = null)
    {
        if (indexes is null)
            indexes = Indexes;

        var tasks = new List<Task>();
        foreach (var idx in indexes)
            tasks.Add(idx.DeleteAsync());

        try
        {
            await Task.WhenAll(tasks).AnyContext();
        }
        finally
        {
            await TryRemoveCacheMarkerAsync().AnyContext();
        }
    }

    public async Task ReindexAsync(IEnumerable<IIndex>? indexes = null, Func<int, string?, Task>? progressCallbackAsync = null)
    {
        if (indexes is null)
            indexes = Indexes;

        var outdatedIndexes = new List<IVersionedIndex>();
        foreach (var versionedIndex in indexes.OfType<IVersionedIndex>())
        {
            int currentVersion = await versionedIndex.GetCurrentVersionAsync().AnyContext();
            if (versionedIndex.Version <= currentVersion)
                continue;

            outdatedIndexes.Add(versionedIndex);
        }

        if (outdatedIndexes.Count == 0)
            return;

        foreach (var outdatedIndex in outdatedIndexes)
        {
            try
            {
                await ResiliencePolicy.ExecuteAsync(async _ =>
                {
                    await outdatedIndex.ReindexAsync((progress, message) =>
                            progressCallbackAsync?.Invoke(progress / outdatedIndexes.Count, message) ?? Task.CompletedTask)
                        .AnyContext();
                }).AnyContext();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to begin reindex for {IndexName} after retries", outdatedIndex.Name);
            }
        }

        await TryRemoveCacheMarkerAsync().AnyContext();
    }

    public async Task<int?> GetServerMajorVersionAsync()
    {
        try
        {
            var response = await Client.InfoAsync().AnyContext();
            _logger.LogRequest(response);
            string? versionNumber = response.IsValidResponse ? response.Version?.Number : null;
            int? major = Index.ParseCreatedMajor(null, versionNumber);
            if (major.HasValue)
                return major;

            _logger.LogWarning("Unable to determine the Elasticsearch server version");
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(ex, "Error getting the Elasticsearch server version: {Message}", ex.Message);
        }

        return null;
    }

    public async Task UpgradeIndexCompatibilityAsync(IEnumerable<IIndex>? indexes = null, Func<int, string?, Task>? progressCallbackAsync = null)
    {
        indexes ??= Indexes;

        foreach (var idx in indexes)
        {
            if (idx is not Index compatibilityIndex)
                continue;

            var workItems = await GetCompatibilityWorkItemsAsync(compatibilityIndex).AnyContext();
            if (workItems.Count == 0)
                continue;

            string lockKey = ElasticReindexer.GetLockName(idx.Name);
            await using var reindexLock = await _lockProvider.AcquireAsync(lockKey, TimeSpan.FromMinutes(20), TimeSpan.FromMinutes(30)).AnyContext();
            if (reindexLock is null)
                throw new RepositoryException($"Unable to acquire the reindex lock for Elasticsearch version compatibility upgrade of index '{idx.Name}'.");

            // Recompute under the lock: another process may have already upgraded these indexes, which would leave
            // the work items above pointing at a stale old index and an already-taken revision name.
            workItems = await GetCompatibilityWorkItemsAsync(compatibilityIndex).AnyContext();
            if (workItems.Count == 0)
                continue;

            await EnsureCompatibilityDestinationsAvailableAsync(workItems).AnyContext();

            var reindexer = new ElasticReindexer(Client, Serializer, TimeProvider, ResiliencePolicyProvider, _logger);
            foreach (var workItem in workItems)
            {
                await compatibilityIndex.CreateCompatibilityIndexAsync(workItem.NewIndex).AnyContext();
                await ResiliencePolicy.ExecuteAsync(async _ =>
                {
                    await reindexLock.RenewAsync().AnyContext();
                    await reindexer.ReindexAsync(workItem, async (progress, message) =>
                    {
                        await reindexLock.RenewAsync().AnyContext();

                        if (progressCallbackAsync is not null)
                            await progressCallbackAsync(progress, message).AnyContext();
                        else
                            _logger.LogInformation("Compatibility reindex {OldIndex} -> {NewIndex} progress {Progress:F1}%: {Message}", workItem.OldIndex, workItem.NewIndex, progress, message);
                    }).AnyContext();
                }).AnyContext();
            }

            await EnsureCompatibilitySourcesRemovedAsync(workItems).AnyContext();

            var remainingWorkItems = await GetCompatibilityWorkItemsAsync(compatibilityIndex).AnyContext();
            if (remainingWorkItems.Count > 0)
            {
                string remainingIndexes = String.Join(", ", remainingWorkItems.Select(w => w.OldIndex));
                throw new RepositoryException($"Elasticsearch version compatibility upgrade for index '{idx.Name}' did not complete. Remaining incompatible indexes: {remainingIndexes}.");
            }
        }
    }

    private string GetConfigureIndexesCacheKey()
    {
        var hasher = new XxHash64();
        foreach (var index in Indexes.OrderBy(i => i.Name))
        {
            hasher.Append(MemoryMarshal.AsBytes(index.Name.AsSpan()));
            if (index is IVersionedIndex v)
            {
                hasher.Append([0xFF]);
                hasher.Append(MemoryMarshal.AsBytes(v.Version.ToString().AsSpan()));
            }
            hasher.Append([0x00]);
        }

        return hasher.GetCurrentHashAsUInt64().ToString("x");
    }

    private async Task<bool> TryCheckCacheMarkerAsync(string cacheKey)
    {
        try
        {
            return await _configureIndexesCache.ExistsAsync(cacheKey).AnyContext();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(ex, "Error checking configure-indexes cache marker: {Message}", ex.Message);
            return false;
        }
    }

    private async Task TrySetCacheMarkerAsync(string cacheKey)
    {
        try
        {
            await _configureIndexesCache.SetAsync(cacheKey, true, TimeSpan.FromMinutes(5)).AnyContext();
            _logger.LogInformation("Index configuration complete, marker set for 5 minutes");
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(ex, "Error setting configure-indexes cache marker: {Message}", ex.Message);
        }
    }

    private async Task TryRemoveCacheMarkerAsync()
    {
        try
        {
            await _configureIndexesCache.RemoveAllAsync().AnyContext();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(ex, "Error removing configure-indexes cache marker: {Message}", ex.Message);
        }
    }

    public virtual void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        // ElasticsearchClientSettings implements IDisposable internally but doesn't expose it
        // on its public API, so we must cast to IDisposable to release its underlying resources.
        if (_client.IsValueCreated)
            (_client.Value.ElasticsearchClientSettings as IDisposable)?.Dispose();

        if (_shouldDisposeCache)
            Cache.Dispose();

        if (_shouldDisposeMessageBus && MessageBus is IDisposable disposableMessageBus)
            disposableMessageBus.Dispose();

        foreach (var index in Indexes)
            index.Dispose();
    }
}
