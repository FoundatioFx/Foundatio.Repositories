using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Foundatio.Jobs;
using Foundatio.Lock;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Extensions;
using Foundatio.Serializer;
using Microsoft.Extensions.Logging;

namespace Foundatio.Repositories.Elasticsearch.Jobs;

/// <summary>
/// Runs a queued <see cref="ReindexWorkItem"/>, serialized against the same distributed lock the direct
/// <see cref="Configuration.VersionedIndex.ReindexAsync"/> path uses.
/// </summary>
public class ReindexWorkItemHandler : WorkItemHandlerBase
{
    private static readonly TimeSpan LockDuration = TimeSpan.FromMinutes(20);
    private static readonly TimeSpan LockAcquireTimeout = TimeSpan.FromMinutes(30);

    private readonly ElasticReindexer _reindexer;
    private readonly ILockProvider _lockProvider;
    private readonly IElasticConfiguration? _configuration;

    /// <summary>
    /// Creates a handler that behaves the same as the direct reindex path: it uses the configuration's
    /// <see cref="TimeProvider"/> and resilience policies, and skips work items whose migration another
    /// process already completed.
    /// </summary>
    public ReindexWorkItemHandler(IElasticConfiguration configuration) : base(GetLoggerFactory(configuration))
    {
        _configuration = configuration;
        _reindexer = new ElasticReindexer(configuration.Client, configuration.Serializer, configuration.TimeProvider, configuration.ResiliencePolicyProvider, configuration.LoggerFactory.CreateLogger<ReindexWorkItemHandler>());
        _lockProvider = configuration.LockProvider;
        AutoRenewLockOnProgress = true;
    }

    /// <summary>
    /// Creates a handler from loose dependencies.
    /// </summary>
    /// <remarks>
    /// Without the configuration this handler cannot tell that a queued work item's migration has already
    /// been completed by another process, so a stale or duplicated work item is reindexed again rather than
    /// skipped. Prefer <see cref="ReindexWorkItemHandler(IElasticConfiguration)"/>.
    /// </remarks>
    public ReindexWorkItemHandler(ElasticsearchClient client, ITextSerializer serializer, ILockProvider lockProvider, ILoggerFactory? loggerFactory = null)
        : base(loggerFactory)
    {
        ArgumentNullException.ThrowIfNull(lockProvider);

        _reindexer = new ElasticReindexer(client, serializer, loggerFactory?.CreateLogger<ReindexWorkItemHandler>());
        _lockProvider = lockProvider;
        AutoRenewLockOnProgress = true;
    }

    private static ILoggerFactory GetLoggerFactory(IElasticConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        return configuration.LoggerFactory;
    }

    /// <summary>
    /// Acquires the reindex lock for the work item's alias, giving up after 30 minutes so a queue worker is
    /// never blocked indefinitely behind another reindex.
    /// </summary>
    /// <returns>
    /// The lock, or <c>null</c> to abandon the work item so the queue redelivers it later. The work item is
    /// abandoned when it is not a <see cref="ReindexWorkItem"/>, when it carries no alias to lock, or when
    /// another reindex holds the lock.
    /// </returns>
    public override async Task<ILock?> GetWorkItemLockAsync(object workItem, CancellationToken cancellationToken = default)
    {
        if (workItem is not ReindexWorkItem reindexWorkItem)
            return null;

        if (String.IsNullOrEmpty(reindexWorkItem.Alias))
        {
            Log.LogWarning("Abandoning reindex work item because it has no alias to lock");
            return null;
        }

        // The interface overload waits until the token is cancelled, so bound the wait to match the direct
        // path instead of parking a queue worker on the lock for as long as the process lives.
        using var acquireTimeoutSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        acquireTimeoutSource.CancelAfter(LockAcquireTimeout);

        return await _lockProvider.TryAcquireAsync(ElasticReindexer.GetLockName(reindexWorkItem.Alias), LockDuration, acquireTimeoutSource.Token).AnyContext();
    }

    public override async Task HandleItemAsync(WorkItemContext context)
    {
        ArgumentNullException.ThrowIfNull(context);

        var workItem = context.GetData<ReindexWorkItem>();
        ArgumentNullException.ThrowIfNull(workItem);

        if (await IsAlreadyReindexedAsync(workItem).AnyContext())
        {
            Log.LogInformation("Skipping queued reindex of {OldIndex} -> {NewIndex}: the index is already at or past that version", workItem.OldIndex, workItem.NewIndex);
            await context.ReportProgressAsync(100, "Already reindexed").AnyContext();
            return;
        }

        await _reindexer.ReindexAsync(workItem, context.ReportProgressAsync, context.CancellationToken).AnyContext();
    }

    /// <summary>
    /// Re-reads the destination index's version now that the lock is held, so a work item whose migration
    /// another process finished while this one sat in the queue is skipped rather than copied again.
    /// </summary>
    /// <remarks>
    /// The index is located by the work item's alias rather than by matching <c>VersionedName</c> against
    /// <see cref="ReindexWorkItem.NewIndex"/>. For a time-series index the work item's destination is a single
    /// dated partition (<c>employees-v2-2026.09.11</c>) while <c>VersionedName</c> is only
    /// <c>employees-v2</c>, so matching on the destination silently never fired for daily and monthly indexes
    /// and this skip did not apply to them at all.
    /// <para>
    /// The comparison stays conservative in the time-series case: a time-series index reports its *lowest*
    /// partition version, so this only skips once every partition has been migrated. A false negative merely
    /// recopies, which converges; a false positive would abandon real work.
    /// </para>
    /// </remarks>
    private async Task<bool> IsAlreadyReindexedAsync(ReindexWorkItem workItem)
    {
        if (_configuration is null)
            return false;

        var versionedIndex = _configuration.Indexes.OfType<IVersionedIndex>()
            .FirstOrDefault(i => String.Equals(i.Name, workItem.Alias, StringComparison.OrdinalIgnoreCase));
        if (versionedIndex is null)
            return false;

        int currentVersion = await versionedIndex.GetCurrentVersionAsync().AnyContext();

        return currentVersion >= versionedIndex.Version;
    }
}
