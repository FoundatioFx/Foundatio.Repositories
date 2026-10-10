using System.Text.Json.Serialization;

namespace Foundatio.Repositories.Elasticsearch;

/// <summary>Why Elasticsearch rejected a document during a reindex.</summary>
public record ReindexFailureCause
{
    /// <summary>Elasticsearch error type, for example <c>mapper_parsing_exception</c>.</summary>
    [JsonPropertyName("type")]
    public string? Type { get; init; }

    /// <summary>Human-readable reason the document was rejected.</summary>
    [JsonPropertyName("reason")]
    public string? Reason { get; init; }

    /// <summary>Server stack trace, when Elasticsearch returned one.</summary>
    [JsonPropertyName("stack_trace")]
    public string? StackTrace { get; init; }
}
