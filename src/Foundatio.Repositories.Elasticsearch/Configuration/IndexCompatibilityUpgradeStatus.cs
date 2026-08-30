using System.Collections.Generic;

namespace Foundatio.Repositories.Elasticsearch.Configuration;

/// <summary>
/// Describes the observed Elasticsearch state and the only safe automatic action for one compatibility upgrade.
/// </summary>
public sealed record IndexCompatibilityUpgradeStatus
{
    /// <summary>The configured repository index name.</summary>
    public required string IndexName { get; init; }

    /// <summary>The exact physical source name supplied by the operator.</summary>
    public required string SourceIndex { get; init; }

    /// <summary>The deterministic compatibility destination for the connected Elasticsearch major.</summary>
    public required string TargetIndex { get; init; }

    /// <summary>The only automatic action supported by the currently observed evidence.</summary>
    public required IndexCompatibilityRecoveryAction Action { get; init; }

    /// <summary>Whether the exact source physical index exists.</summary>
    public required bool SourceExists { get; init; }

    /// <summary>Whether the exact destination physical index exists.</summary>
    public required bool TargetExists { get; init; }

    /// <summary>Whether the source is write blocked.</summary>
    public bool SourceWriteBlocked { get; init; }

    /// <summary>Whether the destination is write blocked.</summary>
    public bool TargetWriteBlocked { get; init; }

    /// <summary>Whether the source carries the compatibility workflow marker.</summary>
    public bool SourceWorkflowMarkerPresent { get; init; }

    /// <summary>Whether the destination carries the compatibility workflow marker.</summary>
    public bool TargetWorkflowMarkerPresent { get; init; }

    /// <summary>Whether the destination has the expected canonical old-physical-name alias.</summary>
    public bool TargetHasCanonicalSourceAlias { get; init; }

    /// <summary>Aliases currently attached to the source.</summary>
    public IReadOnlyCollection<string> SourceAliases { get; init; } = [];

    /// <summary>Aliases currently attached to the destination.</summary>
    public IReadOnlyCollection<string> TargetAliases { get; init; } = [];

    /// <summary>
    /// Concrete indexes unexpectedly resolved by the requested source, deterministic destination, or a marked
    /// destination from another Elasticsearch major. Any value makes automatic recovery unsafe.
    /// </summary>
    public IReadOnlyCollection<string> UnexpectedResolvedIndexes { get; init; } = [];

    /// <summary>
    /// Number of active reindex tasks carrying this operation's exact <c>X-Opaque-Id</c>, or <c>null</c> when
    /// Elasticsearch did not return a complete task listing.
    /// </summary>
    public int? ActiveReindexTaskCount { get; init; }

    /// <summary>Whether the recovery API can apply <see cref="Action"/>.</summary>
    public bool CanRecover => Action is IndexCompatibilityRecoveryAction.Reset or IndexCompatibilityRecoveryAction.Finish;
}

/// <summary>The operator-facing action supported by the observed compatibility-upgrade evidence.</summary>
public enum IndexCompatibilityRecoveryAction
{
    /// <summary>No interrupted Foundatio workflow was observed.</summary>
    None,

    /// <summary>An exactly identified compatibility reindex task is still active; wait and inspect again.</summary>
    Wait,

    /// <summary>Delete the marked partial destination, then unblock and unmark the intact source.</summary>
    Reset,

    /// <summary>Finish a committed cutover by unblocking and unmarking its destination.</summary>
    Finish,

    /// <summary>The evidence is incomplete, foreign, or contradictory; no automatic mutation is safe.</summary>
    ManualIntervention
}
