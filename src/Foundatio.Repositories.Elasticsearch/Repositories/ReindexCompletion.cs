using System;
using System.Text.Json.Serialization;

namespace Foundatio.Repositories.Elasticsearch;

/// <summary>Durable evidence that a physical reindex completed its selected copy contract.</summary>
/// <remarks>
/// Identity is alias/source/destination, with a versioned bounded key. Destination UUID and a fingerprint of
/// script, timestamp field, start range, and quiesce mode prevent reuse for different copy semantics. The record
/// is written after required work and before source cleanup. Alias state or a new request flag cannot recreate
/// missing evidence. A default-mode record attests task completion, not lossless concurrent migration.
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
    /// script, source range, timestamp field, or consistency mode cannot vouch for this one.
    /// </summary>
    [JsonPropertyName("transformation")]
    public required string Transformation { get; init; }

    /// <summary>When the migration was confirmed complete.</summary>
    [JsonPropertyName("completed_utc")]
    public required DateTime CompletedUtc { get; init; }
}
