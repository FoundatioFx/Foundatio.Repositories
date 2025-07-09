using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Elasticsearch.Net;
using Foundatio.Caching;
using Foundatio.Jobs;
using Foundatio.Lock;
using Foundatio.Messaging;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Queues;
using Foundatio.Repositories.Elasticsearch.CustomFields;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Extensions;
using Foundatio.Resilience;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Configuration;

public class ElasticConfiguration : IElasticConfiguration
{
    protected readonly IQueue<WorkItemData> _workItemQueue;
    protected readonly ILogger _logger;
    protected readonly ILockProvider _beginReindexLockProvider;
    protected readonly ILockProvider _lockProvider;
    private readonly List<IIndex> _indexes = new();
    private readonly Lazy<IReadOnlyCollection<IIndex>> _frozenIndexes;
    private readonly Lazy<IElasticClient> _client;
    private readonly Lazy<ICustomFieldDefinitionRepository> _customFieldDefinitionRepository;
    protected readonly bool _shouldDisposeCache;

    public ElasticConfiguration(IQueue<WorkItemData> workItemQueue = null, ICacheClient cacheClient = null, IMessageBus messageBus = null, TimeProvider timeProvider = null, IResiliencePolicyProvider resiliencePolicyProvider = null, ILoggerFactory loggerFactory = null)
    {
        _workItemQueue = workItemQueue;
        TimeProvider = timeProvider ?? TimeProvider.System;
        LoggerFactory = loggerFactory ?? NullLoggerFactory.Instance;
        _logger = LoggerFactory.CreateLogger(GetType());
        ResiliencePolicyProvider = resiliencePolicyProvider ?? cacheClient?.GetResiliencePolicyProvider() ?? new ResiliencePolicyProvider();
        ResiliencePolicy = ResiliencePolicyProvider.GetPolicy<ElasticConfiguration>(_logger, TimeProvider);
        Cache = cacheClient ?? new InMemoryCacheClient(new InMemoryCacheClientOptions { CloneValues = true, ResiliencePolicyProvider = ResiliencePolicyProvider, TimeProvider = TimeProvider, LoggerFactory = LoggerFactory });
        _lockProvider = new CacheLockProvider(Cache, messageBus, TimeProvider, ResiliencePolicyProvider, LoggerFactory);
        _beginReindexLockProvider = new ThrottlingLockProvider(Cache, 1, TimeSpan.FromMinutes(15), TimeProvider, ResiliencePolicyProvider, LoggerFactory);
        _shouldDisposeCache = cacheClient == null;
        MessageBus = messageBus ?? new InMemoryMessageBus(new InMemoryMessageBusOptions { ResiliencePolicyProvider = ResiliencePolicyProvider, TimeProvider = TimeProvider, LoggerFactory = LoggerFactory });
        _frozenIndexes = new Lazy<IReadOnlyCollection<IIndex>>(() => _indexes.AsReadOnly());
        _customFieldDefinitionRepository = new Lazy<ICustomFieldDefinitionRepository>(CreateCustomFieldDefinitionRepository);
        _client = new Lazy<IElasticClient>(CreateElasticClient);
    }

    protected virtual IElasticClient CreateElasticClient()
    {
        var settings = new ConnectionSettings(CreateConnectionPool() ?? new SingleNodeConnectionPool(new Uri("http://localhost:9200")));
        settings.EnableApiVersioningHeader();
        ConfigureSettings(settings);
        foreach (var index in Indexes)
            index.ConfigureSettings(settings);

        return new ElasticClient(settings);
    }

    public virtual void ConfigureGlobalQueryBuilders(ElasticQueryBuilder builder) { }

    public virtual void ConfigureGlobalQueryParsers(ElasticQueryParserConfiguration config) { }

    protected virtual void ConfigureSettings(ConnectionSettings settings)
    {
        settings.EnableTcpKeepAlive(TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(2));
    }

    protected virtual IConnectionPool CreateConnectionPool()
    {
        return null;
    }

    public IElasticClient Client => _client.Value;
    public ICacheClient Cache { get; }
    public IMessageBus MessageBus { get; }
    public ILoggerFactory LoggerFactory { get; }
    public IResiliencePolicyProvider ResiliencePolicyProvider { get; }
    public IResiliencePolicy ResiliencePolicy { get; }
    public TimeProvider TimeProvider { get; set; }
    public IReadOnlyCollection<IIndex> Indexes => _frozenIndexes.Value;
    public ICustomFieldDefinitionRepository CustomFieldDefinitionRepository => _customFieldDefinitionRepository.Value;

    private CustomFieldDefinitionIndex _customFieldDefinitionIndex = null;
    private ICustomFieldDefinitionRepository CreateCustomFieldDefinitionRepository()
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

        _indexes.Add(index);
    }

    public IIndex GetIndex(string name)
    {
        foreach (var index in Indexes)
            if (index.Name.Equals(name, StringComparison.OrdinalIgnoreCase))
                return index;

        return null;
    }

    public Task ConfigureIndexesAsync(IEnumerable<IIndex> indexes = null, bool beginReindexingOutdated = true)
    {
        if (indexes == null)
            indexes = Indexes;

        var tasks = new List<Task>();
        foreach (var idx in indexes)
            tasks.Add(ConfigureIndexInternalAsync(idx, beginReindexingOutdated));

        return Task.WhenAll(tasks);
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
        bool isReindexing = await _lockProvider.IsLockedAsync(String.Join(":", "reindex", reindexWorkItem.Alias,
            reindexWorkItem.OldIndex, reindexWorkItem.NewIndex)).AnyContext();
        if (isReindexing)
            return;

        // enqueue reindex to new version, only allowed every 15 minutes
        string enqueueReindexLockName = String.Join(":", "enqueue-reindex", reindexWorkItem.Alias, reindexWorkItem.OldIndex, reindexWorkItem.NewIndex);
        await _beginReindexLockProvider.TryUsingAsync(enqueueReindexLockName, () => _workItemQueue.EnqueueAsync(reindexWorkItem), TimeSpan.Zero, new CancellationToken(true)).AnyContext();
    }

    public Task MaintainIndexesAsync(IEnumerable<IIndex> indexes = null)
    {
        if (indexes == null)
            indexes = Indexes;

        var tasks = new List<Task>();
        foreach (var idx in indexes)
            tasks.Add(idx.MaintainAsync());

        return Task.WhenAll(tasks);
    }

    public Task DeleteIndexesAsync(IEnumerable<IIndex> indexes = null)
    {
        if (indexes == null)
            indexes = Indexes;

        var tasks = new List<Task>();
        foreach (var idx in indexes)
            tasks.Add(idx.DeleteAsync());

        return Task.WhenAll(tasks);
    }

    public async Task ReindexAsync(IEnumerable<IIndex> indexes = null, Func<int, string, Task> progressCallbackAsync = null)
    {
        if (indexes == null)
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
                await ResiliencePolicy.ExecuteAsync(async () =>
                {
                    await outdatedIndex.ReindexAsync((progress, message) =>
                            progressCallbackAsync?.Invoke(progress / outdatedIndexes.Count, message) ?? Task.CompletedTask)
                        .AnyContext();
                }).AnyContext();
            }
            catch (Exception)
            {
                // unable to reindex after 5 retries, move to next index.
            }
        }
    }

    public virtual void Dispose()
    {
        if (_shouldDisposeCache)
            Cache.Dispose();

        foreach (var index in Indexes)
            index.Dispose();
    }
}
