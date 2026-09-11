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

        var disposition = await GetRedeliveryDispositionAsync(workItem, context.CancellationToken).AnyContext();
        switch (disposition)
        {
            case RedeliveryDisposition.AlreadyCompleted:
                Log.LogInformation("Skipping queued reindex of {OldIndex} -> {NewIndex}: this migration is recorded as complete", workItem.OldIndex, workItem.NewIndex);
                await context.ReportProgressAsync(100, "Already reindexed").AnyContext();
                return;

            case RedeliveryDisposition.PromotedButUnconfirmed:
                // Neither success nor permission to copy again. The alias already points at the destination, so
                // acknowledging this would record a possibly short index as a finished migration, and copying
                // again would write into an index that is already serving live traffic. Throwing hands the item
                // to the queue's own abandon/retry/dead-letter handling with the work identity intact, and both
                // indexes are left untouched for recovery.
                Log.LogError("Queued reindex of {OldIndex} -> {NewIndex} found alias {Alias} already promoted with no completion record. Not acknowledging and not recopying; both indexes are left in place.",
                    workItem.OldIndex, workItem.NewIndex, workItem.Alias);
                throw new ReindexCompletionUnknownException(workItem.Alias, workItem.OldIndex, workItem.NewIndex,
                    "the alias already points at the destination but no completion record vouches for this migration");

            case RedeliveryDisposition.SafeToStart:
            default:
                await _reindexer.ReindexAsync(workItem, context.ReportProgressAsync, context.CancellationToken).AnyContext();
                return;
        }
    }

    /// <summary>
    /// What a redelivered work item is allowed to do, given what can be observed about the migration.
    /// </summary>
    private enum RedeliveryDisposition
    {
        /// <summary>The migration has not been promoted, so the copy can run.</summary>
        SafeToStart,

        /// <summary>A completion record vouches for this migration, so the duplicate can be acknowledged.</summary>
        AlreadyCompleted,

        /// <summary>
        /// The destination is promoted but nothing vouches for the migration, so the outcome is unknown.
        /// </summary>
        PromotedButUnconfirmed
    }

    /// <summary>
    /// Decides what a redelivered work item may do, separating "work may safely start" from "already promoted,
    /// completion unknown".
    /// </summary>
    /// <remarks>
    /// The distinction matters because those two states used to be conflated. An advanced alias was read as
    /// proof the migration had finished, so a first attempt that failed after the cutover - the alias moves
    /// before the catch-up pass - was acknowledged as a completed migration on redelivery. Completion is
    /// therefore established only by a matching completion record, never inferred from alias state, document
    /// counts, or a progress report.
    /// </remarks>
    private async Task<RedeliveryDisposition> GetRedeliveryDispositionAsync(ReindexWorkItem workItem, CancellationToken cancellationToken)
    {
        if (!await IsAlreadyPromotedAsync(workItem).AnyContext())
            return RedeliveryDisposition.SafeToStart;

        if (await _reindexer.HasCompletionEvidenceAsync(workItem, cancellationToken).AnyContext())
            return RedeliveryDisposition.AlreadyCompleted;

        return RedeliveryDisposition.PromotedButUnconfirmed;
    }

    /// <summary>
    /// Returns whether the alias already points at this migration's target version, which means the cutover has
    /// happened - but says nothing about whether the migration finished.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This deliberately answers only "has the destination been promoted?". It used to be read as "has this
    /// already been reindexed?", which conflated a migration another process completed with one that promoted
    /// the destination and then failed; the alias switch happens before the catch-up pass, so both leave the
    /// version advanced. Completion is established separately by a completion record.
    /// </para>
    /// <para>
    /// The index is located by the work item's alias rather than by matching <c>VersionedName</c> against
    /// <see cref="ReindexWorkItem.NewIndex"/>. For a time-series index the work item's destination is a single
    /// dated partition (<c>employees-v2-2026.09.11</c>) while <c>VersionedName</c> is only
    /// <c>employees-v2</c>, so matching on the destination silently never fired for daily and monthly indexes.
    /// </para>
    /// <para>
    /// A time-series index reports its <em>lowest</em> partition version, so this only reports promotion once
    /// every partition has moved.
    /// </para>
    /// </remarks>
    private async Task<bool> IsAlreadyPromotedAsync(ReindexWorkItem workItem)
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
