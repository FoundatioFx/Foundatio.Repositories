namespace Foundatio.Repositories.Elasticsearch.Configuration;

/// <summary>
/// Classifies whether a physical index can be carried forward to the next Elasticsearch major version.
/// </summary>
public enum IndexCompatibilityState
{
    /// <summary>The index was created by the connected Elasticsearch major version.</summary>
    Current,

    /// <summary>The index was created by the immediately previous major and must be reindexed before the next major upgrade.</summary>
    RequiresReindex,

    /// <summary>The index is more than one major behind the connected server and cannot be upgraded by this workflow.</summary>
    Unsupported
}

/// <summary>
/// Describes the Elasticsearch version compatibility of a single physical index backing an <see cref="IIndex"/>.
/// </summary>
public record IndexCompatibilityInfo
{
    /// <summary>
    /// The concrete physical index name (e.g. <c>employees</c>, <c>employees-v1</c>, or <c>logs-v1-2024.01.15</c>).
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// The major Elasticsearch version that created <see cref="Name"/> (parsed from <c>index.version.created</c>).
    /// The operation fails when this value cannot be determined.
    /// </summary>
    public required int CreatedMajor { get; init; }

    /// <summary>
    /// The Elasticsearch version string that created <see cref="Name"/> (e.g. <c>"7.17.19"</c>), or <c>null</c>
    /// if it could not be determined.
    /// </summary>
    public string? CreatedVersion { get; init; }

    /// <summary>
    /// The major version of the connected Elasticsearch server when compatibility was checked.
    /// </summary>
    public required int ServerMajor { get; init; }

    /// <summary>
    /// The connected Elasticsearch server version when compatibility was checked.
    /// </summary>
    public required string ServerVersion { get; init; }

    /// <summary>
    /// Gets the compatibility classification derived from <see cref="CreatedMajor"/> and <see cref="ServerMajor"/>.
    /// </summary>
    public IndexCompatibilityState State => CreatedMajor == ServerMajor
        ? IndexCompatibilityState.Current
        : CreatedMajor == ServerMajor - 1
            ? IndexCompatibilityState.RequiresReindex
            : IndexCompatibilityState.Unsupported;

    /// <summary>
    /// Gets whether the index was created by the immediately previous Elasticsearch major and must be reindexed
    /// before upgrading the server again.
    /// </summary>
    public bool RequiresReindexBeforeNextMajorUpgrade => State is IndexCompatibilityState.RequiresReindex;
}
