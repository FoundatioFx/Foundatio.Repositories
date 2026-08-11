using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Foundatio.Repositories.Elasticsearch.Configuration;

/// <summary>
/// Exposes Elasticsearch major-version compatibility operations for a repository configuration.
/// </summary>
public interface IElasticConfigurationCompatibility : IElasticConfiguration
{
    /// <summary>
    /// Inspects the physical source, deterministic destination, aliases, write blocks, and active reindex tasks
    /// without changing cluster state.
    /// </summary>
    /// <param name="index">The configured index that owns <paramref name="sourceIndex"/>.</param>
    /// <param name="sourceIndex">The canonical concrete source name used to start the compatibility upgrade.</param>
    /// <param name="cancellationToken">The token used to cancel the inspection.</param>
    Task<IndexCompatibilityUpgradeStatus> InspectIndexCompatibilityUpgradeAsync(
        IIndex index,
        string sourceIndex,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Conservatively resets an interrupted pre-cutover attempt after independently confirming that no reindex
    /// task is active. It may delete an unaliased destination and optionally remove the source write block.
    /// Completed or ambiguous cutovers are never changed.
    /// </summary>
    /// <param name="index">The configured index that owns <paramref name="sourceIndex"/>.</param>
    /// <param name="sourceIndex">The canonical concrete source name used to start the compatibility upgrade.</param>
    /// <param name="removeWriteBlock">
    /// Whether to remove the write block from the surviving source or completed destination. Set this only after
    /// verifying that the block was added by the interrupted operation rather than by an administrator.
    /// </param>
    /// <param name="cancellationToken">The token used to acquire the lock and perform recovery.</param>
    Task<IndexCompatibilityUpgradeStatus> RecoverIndexCompatibilityUpgradeAsync(
        IIndex index,
        string sourceIndex,
        bool removeWriteBlock = false,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Recreates physical indexes written by an older Elasticsearch major version using the connected server's
    /// index format. Indexes that don't derive from <see cref="Index"/> are rejected; compatible indexes are skipped.
    /// The operation verifies that no incompatible physical indexes remain before returning.
    /// </summary>
    /// <remarks>
    /// This is an offline maintenance operation. It blocks writes, creates an exact replacement with
    /// Elasticsearch's <c>_create_from</c> API, copies documents without applying ingest pipelines, atomically
    /// moves aliases, and deletes the source. Stop all writers and index-management processes, take and verify a
    /// snapshot, and do not invoke it while rollback to the previous Elasticsearch major remains an option.
    /// Restart or drain application instances before resuming writes because cached document concurrency tokens
    /// belong to the deleted physical index.
    /// </remarks>
    /// <param name="indexes">The indexes to inspect and upgrade, or <c>null</c> for all configured indexes.</param>
    /// <param name="progressCallbackAsync">An optional callback for per-index progress updates.</param>
    /// <param name="cancellationToken">The token used to cancel detection or pre-cutover work.</param>
    /// <exception cref="Foundatio.Repositories.Exceptions.RepositoryException">The compatibility upgrade did not complete.</exception>
    /// <exception cref="ArgumentException">An index belongs to a different configuration.</exception>
    /// <exception cref="NotSupportedException">An index implementation is unsupported or the connected Elasticsearch version is older than 8.18.</exception>
    Task UpgradeIndexCompatibilityAsync(IEnumerable<IIndex>? indexes = null, Func<int, string?, Task>? progressCallbackAsync = null, CancellationToken cancellationToken = default);
}
