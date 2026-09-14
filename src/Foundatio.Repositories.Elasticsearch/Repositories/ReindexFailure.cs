using System;
using System.Text.Json.Serialization;

namespace Foundatio.Repositories.Elasticsearch;

/// <summary>
/// A document that could not be copied during a reindex, recorded in the <c>{destination}-error</c> index so
/// the documents left behind can be found and replayed after the fact.
/// </summary>
/// <remarks>
/// The field names are explicit because the index mapping is explicit: this type and
/// <c>ElasticReindexer.CreateFailureIndexAsync</c> describe the same documents, and a rename in one that is
/// not mirrored in the other would silently produce unsearchable failure records again.
/// </remarks>
public record ReindexFailure
{
    /// <summary>
    /// Index the write was attempted against, as reported by Elasticsearch. This is the reindex destination,
    /// not where the document can be read from - see <see cref="SourceIndex"/> for that.
    /// </summary>
    [JsonPropertyName("index")]
    public string? Index { get; init; }

    /// <summary>Index the document should be replayed from.</summary>
    [JsonPropertyName("source_index")]
    public string? SourceIndex { get; init; }

    /// <summary>Id of the document that failed to copy. Use this to replay it.</summary>
    [JsonPropertyName("id")]
    public string? Id { get; init; }

    /// <summary>Version of the source document at the time of the failure.</summary>
    [JsonPropertyName("version")]
    public long? Version { get; init; }

    /// <summary>Routing value of the source document, required to replay a routed document.</summary>
    [JsonPropertyName("routing")]
    public string? Routing { get; init; }

    /// <summary>HTTP status Elasticsearch returned for the failed write.</summary>
    [JsonPropertyName("status")]
    public int Status { get; init; }

    /// <summary>Whether the source document could still be read when the failure was recorded.</summary>
    [JsonPropertyName("found")]
    public bool Found { get; init; }

    /// <summary>When the failure was recorded, so failures can be correlated with a specific reindex run.</summary>
    [JsonPropertyName("created_utc")]
    public DateTime CreatedUtc { get; init; }

    /// <summary>Why Elasticsearch rejected the document.</summary>
    [JsonPropertyName("cause")]
    public ReindexFailureCause? Cause { get; init; }

    /// <summary>
    /// The source document, stored for replay but deliberately not indexed - its shape is arbitrary, and
    /// indexing it would risk mapping conflicts in the index being relied on for recovery.
    /// </summary>
    [JsonPropertyName("source")]
    public object? Source { get; init; }
}
