using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Foundatio.Repositories.Elasticsearch.Configuration;

/// <summary>
/// Exposes Elasticsearch major-version compatibility detection for an index.
/// </summary>
public interface IIndexCompatibility : IIndex
{
    /// <summary>
    /// Checks the Elasticsearch version compatibility of every structurally valid physical index owned by this
    /// index, including retained versioned indexes and expired time-series partitions.
    /// </summary>
    /// <param name="cancellationToken">The token used to cancel the server-info or index-settings request.</param>
    /// <returns>One result per current physical index, or an empty collection when no physical index exists.</returns>
    /// <exception cref="Foundatio.Repositories.Exceptions.RepositoryException">The current Elasticsearch server version or an index's creation version could not be determined.</exception>
    Task<IReadOnlyCollection<IndexCompatibilityInfo>> GetIndexCompatibilityAsync(CancellationToken cancellationToken = default);
}
