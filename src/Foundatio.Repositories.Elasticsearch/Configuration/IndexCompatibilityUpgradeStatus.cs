using System.Collections.Generic;

namespace Foundatio.Repositories.Elasticsearch.Configuration;

/// <summary>
/// Describes the observable Elasticsearch topology for one compatibility-upgrade source and its deterministic destination.
/// </summary>
public sealed record IndexCompatibilityUpgradeStatus
{
    /// <summary>The configured repository index name.</summary>
    public required string IndexName { get; init; }

    /// <summary>The concrete physical source name supplied by the operator.</summary>
    public required string SourceIndex { get; init; }

    /// <summary>The deterministic compatibility destination name for the connected Elasticsearch major.</summary>
    public required string TargetIndex { get; init; }

    /// <summary>The recovery classification derived from physical-index and alias topology.</summary>
    public required IndexCompatibilityUpgradeRecoveryState State { get; init; }

    /// <summary>Whether the concrete source still exists.</summary>
    public required bool SourceExists { get; init; }

    /// <summary>Whether the deterministic destination exists.</summary>
    public required bool TargetExists { get; init; }

    /// <summary>Whether the source currently has <c>index.blocks.write</c> enabled.</summary>
    public bool SourceWriteBlocked { get; init; }

    /// <summary>Whether the destination currently has <c>index.blocks.write</c> enabled.</summary>
    public bool TargetWriteBlocked { get; init; }

    /// <summary>Aliases currently attached to the source.</summary>
    public IReadOnlyCollection<string> SourceAliases { get; init; } = [];

    /// <summary>Aliases currently attached to the destination.</summary>
    public IReadOnlyCollection<string> TargetAliases { get; init; } = [];

    /// <summary>
    /// Number of active cluster-wide reindex tasks, or <c>null</c> when task state could not be established.
    /// Recovery fails closed unless this is zero.
    /// </summary>
    public int? ActiveReindexTaskCount { get; init; }

    /// <summary>Whether the topology is safe for the explicit conservative recovery operation.</summary>
    public bool CanRecover => ActiveReindexTaskCount is 0
        && State is IndexCompatibilityUpgradeRecoveryState.Interrupted
            or IndexCompatibilityUpgradeRecoveryState.SourceWriteBlocked
            or IndexCompatibilityUpgradeRecoveryState.CompletedWriteBlocked;
}

/// <summary>Classifies the observable state of a compatibility upgrade.</summary>
public enum IndexCompatibilityUpgradeRecoveryState
{
    /// <summary>The source exists, is writable, and the destination does not exist.</summary>
    Ready,

    /// <summary>The source exists and is write blocked, but the destination does not exist.</summary>
    SourceWriteBlocked,

    /// <summary>Both indexes exist, the destination has no aliases, and no active reindex task was observed.</summary>
    Interrupted,

    /// <summary>Both indexes exist and at least one reindex task is active.</summary>
    InProgress,

    /// <summary>The source is gone and the destination has the canonical source alias, consistent with completed cutover.</summary>
    Completed,

    /// <summary>The source is gone and the destination exists with aliases but remains write blocked.</summary>
    CompletedWriteBlocked,

    /// <summary>Neither concrete index exists.</summary>
    Missing,

    /// <summary>The observed topology is not safe for automatic recovery.</summary>
    Ambiguous
}
