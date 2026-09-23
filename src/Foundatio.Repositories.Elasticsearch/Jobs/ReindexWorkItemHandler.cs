using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Foundatio.Jobs;
using Foundatio.Lock;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Exceptions;
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
    private readonly ElasticsearchClient _client;

    /// <summary>
    /// Creates a handler that behaves the same as the direct reindex path: it uses the configuration's
    /// <see cref="TimeProvider"/> and resilience policies, and skips work items whose migration another
    /// process already completed.
    /// </summary>
    public ReindexWorkItemHandler(IElasticConfiguration configuration) : base(GetLoggerFactory(configuration))
    {
        _client = configuration.Client;
        _reindexer = new ElasticReindexer(configuration.Client, configuration.Serializer, configuration.TimeProvider, configuration.ResiliencePolicyProvider, configuration.LoggerFactory.CreateLogger<ReindexWorkItemHandler>());
        _lockProvider = configuration.LockProvider;
        AutoRenewLockOnProgress = true;
    }

    /// <summary>
    /// Creates a handler from loose dependencies.
    /// </summary>
    /// <remarks>
    /// Redelivery is checked against the work item's physical destination and durable completion record,
    /// without requiring a registered index configuration. Prefer
    /// <see cref="ReindexWorkItemHandler(IElasticConfiguration)"/> to also use its time provider and resilience policies.
    /// </remarks>
    public ReindexWorkItemHandler(ElasticsearchClient client, ITextSerializer serializer, ILockProvider lockProvider, ILoggerFactory? loggerFactory = null)
        : base(loggerFactory)
    {
        ArgumentNullException.ThrowIfNull(lockProvider);

        _reindexer = new ElasticReindexer(client, serializer, loggerFactory?.CreateLogger<ReindexWorkItemHandler>());
        _client = client;
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

        await using var guard = context.WorkItemLock is null ? null : new ReindexLeaseGuard(context.WorkItemLock, TimeProvider.System);
        using var guardedCancellation = CancellationTokenSource.CreateLinkedTokenSource(context.CancellationToken, guard?.LostToken ?? CancellationToken.None);
        var cancellationToken = guardedCancellation.Token;
        var disposition = await GetRedeliveryDispositionAsync(workItem, cancellationToken).AnyContext();
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
                await _reindexer.ReindexAsync(workItem, context.ReportProgressAsync, cancellationToken).AnyContext();
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
    /// <para>
    /// A matching completion record is checked even when the alias has since moved to another version.
    /// Neither the worker's configured schema version nor the lowest version of other time-series partitions
    /// describes whether this particular destination has been promoted.
    /// </para>
    /// <para>
    /// When no record matches, promotion is checked against the exact destination in the work item. A work-item flag is not evidence that the promoted generation was actually verified.
    /// </para>
    /// </remarks>
    private async Task<RedeliveryDisposition> GetRedeliveryDispositionAsync(ReindexWorkItem workItem, CancellationToken cancellationToken)
    {
        if (await _reindexer.HasCompletionEvidenceAsync(workItem, cancellationToken).AnyContext())
            return RedeliveryDisposition.AlreadyCompleted;

        if (!await IsAlreadyPromotedAsync(_client, workItem, cancellationToken).AnyContext())
            return RedeliveryDisposition.SafeToStart;

        return RedeliveryDisposition.PromotedButUnconfirmed;
    }

    /// <summary>
    /// Returns whether the work item's alias is attached to its exact physical destination, including a dated
    /// partition. Alias state alone does not establish completion.
    /// </summary>
    /// <exception cref="RepositoryException">
    /// The destination's alias state could not be read completely. An unreadable response is not permission
    /// to copy into a destination that may already be serving traffic.
    /// </exception>
    internal static async Task<bool> IsAlreadyPromotedAsync(ElasticsearchClient client, ReindexWorkItem workItem, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(workItem);
        ArgumentException.ThrowIfNullOrWhiteSpace(workItem.NewIndex);
        ArgumentException.ThrowIfNullOrWhiteSpace(workItem.Alias);
        cancellationToken.ThrowIfCancellationRequested();

        var response = await client.Indices.GetAliasAsync(Indices.Index(workItem.NewIndex), cancellationToken).AnyContext();
        if (!response.IsValidResponse)
        {
            if (response.ApiCallDetails.HttpStatusCode is 404)
                return false;

            throw new RepositoryException(response.GetErrorMessage($"Could not read aliases for reindex destination {workItem.NewIndex}"), response.OriginalException());
        }

#if ELASTICSEARCH9
        var indices = response.Aliases;
#else
        var indices = response.Values;
#endif
        if (indices is null || indices.Count is not 1)
            throw new RepositoryException($"The alias response did not identify the exact reindex destination {workItem.NewIndex}");

        var destination = indices.First();
        if (!String.Equals(destination.Key, workItem.NewIndex) || destination.Value?.Aliases is null)
            throw new RepositoryException($"The alias response did not describe the exact reindex destination {workItem.NewIndex}");

        return destination.Value.Aliases.ContainsKey(workItem.Alias);
    }
}
