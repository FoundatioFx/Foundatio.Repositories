using System;
using System.Text.Json.Serialization;

namespace Foundatio.Repositories.Elasticsearch;

/// <summary>
/// Durable evidence that one specific physical reindex actually finished, written only after the copy's own
/// accounting confirmed it.
/// </summary>
/// <remarks>
/// <para>
/// This exists because an advanced alias is not evidence of completion. The alias switch happens before the
/// catch-up pass, so a promoted destination is equally consistent with "the migration finished" and "the
/// migration promoted the destination and then failed". Both leave the same observable cluster state, so a
/// redelivered work item cannot tell them apart from the alias, the document counts, or a progress report.
/// This record is the only thing that can.
/// </para>
/// <para>
/// It is bound to the <em>physical</em> migration, not to an attempt: the identity is the alias plus the
/// source and destination index names, so the same logical migration is recognizable across redelivery,
/// process restart, and direct-versus-queued execution. <see cref="DestinationUuid"/> pins it to one
/// generation of the destination index, so a record left behind by an earlier index that has since been
/// deleted and recreated under the same name does not satisfy the check.
/// <see cref="Transformation"/> pins it to the reindex script, so a record written for a different
/// transformation does not satisfy it either.
/// </para>
/// <para>
/// <strong>What this does not attest.</strong> It records that the reindex met the contract this library
/// implements - the copy task reported that it matched and wrote every document it set out to, the catch-up
/// pass completed or was proven unnecessary, and the aliases moved. It is not proof of strict or lossless
/// consistency: writes that land on the source after the catch-up pass and before the cutover are still
/// outside the guarantee for models that can be caught up. See the reindex guide's remaining limitations.
/// </para>
/// </remarks>
public record ReindexCompletion
{
    /// <summary>Alias whose migration this records.</summary>
    [JsonPropertyName("alias")]
    public required string Alias { get; init; }

    /// <summary>Index documents were copied from. Recorded for diagnostics; it may since have been deleted.</summary>
    [JsonPropertyName("source_index")]
    public required string SourceIndex { get; init; }

    /// <summary>Index documents were copied to.</summary>
    [JsonPropertyName("destination_index")]
    public required string DestinationIndex { get; init; }

    /// <summary>
    /// The destination index's Elasticsearch UUID, which identifies one physical generation of that name.
    /// </summary>
    [JsonPropertyName("destination_uuid")]
    public required string DestinationUuid { get; init; }

    /// <summary>
    /// Fingerprint of the transformation contract the copy ran under, so a record written for a different
    /// reindex script cannot vouch for this one.
    /// </summary>
    [JsonPropertyName("transformation")]
    public required string Transformation { get; init; }

    /// <summary>When the migration was confirmed complete.</summary>
    [JsonPropertyName("completed_utc")]
    public required DateTime CompletedUtc { get; init; }
}
