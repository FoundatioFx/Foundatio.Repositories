using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Foundatio.Repositories.Elasticsearch.Configuration;

/// <summary>
/// Exposes Elasticsearch major-version compatibility operations for a repository configuration.
/// </summary>
public interface IElasticConfigurationCompatibility : IElasticConfiguration
{
    /// <summary>
    /// Returns the major version number of the connected Elasticsearch server (e.g. <c>9</c> for <c>9.0.1</c>),
    /// or <c>null</c> if it could not be determined. Each call reads the connected server's current version.
    /// </summary>
    Task<int?> GetServerMajorVersionAsync();

    /// <summary>
    /// Reindexes physical indexes that were created by an Elasticsearch major version older than the connected
    /// server's, so they don't become unreadable after the next major upgrade. Indexes that don't derive from
    /// <see cref="Index"/> or don't require an upgrade are skipped. The operation verifies that no incompatible
    /// physical indexes remain before returning.
    /// </summary>
    /// <remarks>
    /// This creates new physical indexes under the connected server version and deletes the old physical indexes.
    /// Take and verify a snapshot first, and do not invoke this operation while rollback to the previous
    /// Elasticsearch major version remains a deployment option.
    /// </remarks>
    /// <exception cref="Foundatio.Repositories.Exceptions.RepositoryException">The compatibility upgrade did not complete.</exception>
    Task UpgradeIndexCompatibilityAsync(IEnumerable<IIndex>? indexes = null, Func<int, string?, Task>? progressCallbackAsync = null);
}
