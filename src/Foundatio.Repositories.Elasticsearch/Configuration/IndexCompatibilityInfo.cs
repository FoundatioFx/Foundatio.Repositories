namespace Foundatio.Repositories.Elasticsearch.Configuration;

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
    /// <c>true</c> when <see cref="CreatedMajor"/> is older than the major version of the currently connected
    /// Elasticsearch server and should be reindexed before the server is upgraded to its next major version.
    /// </summary>
    public bool RequiresReindexBeforeNextMajorUpgrade { get; init; }
}
