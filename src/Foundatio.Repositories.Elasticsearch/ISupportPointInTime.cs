using System.Threading.Tasks;
using Foundatio.Utility;

namespace Foundatio.Repositories.Elasticsearch;

/// <summary>
/// Indicates the repository supports Elasticsearch point-in-time snapshots for consistent search-after cursor paging.
/// </summary>
public interface ISupportPointInTime
{
    /// <summary>Closes a point-in-time snapshot. Returns <c>true</c> if it was closed; <c>false</c> if <paramref name="pointInTimeId"/> is null or empty, or the snapshot could not be closed.</summary>
    Task<bool> ClosePointInTimeAsync(string? pointInTimeId);

    /// <summary>Closes the point-in-time snapshot stored in the result data. Returns <c>true</c> if it was closed; <c>false</c> if <paramref name="results"/> is null or carries no point-in-time id, or the snapshot could not be closed.</summary>
    Task<bool> ClosePointInTimeAsync(IHaveData? results);
}
