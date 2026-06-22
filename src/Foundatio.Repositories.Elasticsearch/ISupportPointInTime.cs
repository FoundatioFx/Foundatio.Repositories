using System.Threading.Tasks;
using Foundatio.Utility;

namespace Foundatio.Repositories.Elasticsearch;

/// <summary>
/// Indicates the repository supports Elasticsearch point-in-time snapshots for consistent search-after cursor paging.
/// </summary>
public interface ISupportPointInTime
{
    /// <summary>Closes a point-in-time snapshot. Returns true if successfully closed.</summary>
    Task<bool> ClosePointInTimeAsync(string? pointInTimeId);

    /// <summary>Closes the point-in-time snapshot stored in the result data. Returns true if successfully closed.</summary>
    Task<bool> ClosePointInTimeAsync(IHaveData? results);
}
