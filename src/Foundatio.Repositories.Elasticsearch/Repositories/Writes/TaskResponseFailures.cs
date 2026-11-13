using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;

namespace Foundatio.Repositories.Elasticsearch;

/// <summary>
/// Reads the per-document failures of a completed update-by-query task response (the <c>failures</c> array).
/// </summary>
internal static class TaskResponseFailures
{
    private const int MaxReported = 5;

    public static bool IsTimedOut(object? response)
    {
        object? timedOut = response switch
        {
            JsonElement { ValueKind: JsonValueKind.Object } element when element.TryGetProperty("timed_out", out var value) => value,
            IDictionary<string, object> dict when dict.TryGetValue("timed_out", out var value) => value,
            _ => null
        };

        return timedOut is true or JsonElement { ValueKind: JsonValueKind.True };
    }

    /// <summary>
    /// Returns one description per failure in the task response. The response is <c>object?</c> and may be a
    /// <see cref="JsonElement"/> or a dictionary depending on the serializer configuration.
    /// </summary>
    public static IReadOnlyList<string> Read(object? response)
    {
        object? failures = response switch
        {
            JsonElement { ValueKind: JsonValueKind.Object } element when element.TryGetProperty("failures", out var value) => value,
            IDictionary<string, object> dict when dict.TryGetValue("failures", out var value) => value,
            _ => null
        };

        return failures switch
        {
            JsonElement { ValueKind: JsonValueKind.Array } array => array.EnumerateArray().Select(Describe).ToList(),
            string or null => [],
            IEnumerable items => items.Cast<object?>().Select(i => i is JsonElement e ? Describe(e) : i?.ToString() ?? "unknown failure").ToList(),
            _ => []
        };
    }

    /// <summary>Formats at most the first few failures for an exception message.</summary>
    public static string Summarize(IReadOnlyList<string> failures)
    {
        string summary = String.Join("; ", failures.Take(MaxReported));
        return failures.Count > MaxReported ? $"{summary}; and {failures.Count - MaxReported} more" : summary;
    }

    private static string Describe(JsonElement failure)
    {
        if (failure.ValueKind is not JsonValueKind.Object)
            return failure.GetRawText();

        string? id = failure.TryGetProperty("id", out var idElement) && idElement.ValueKind is JsonValueKind.String ? idElement.GetString() : null;
        if (!failure.TryGetProperty("cause", out var cause) || cause.ValueKind is not JsonValueKind.Object)
            return id is null ? failure.GetRawText() : $"{id}: {failure.GetRawText()}";

        string type = cause.TryGetProperty("type", out var typeElement) && typeElement.ValueKind is JsonValueKind.String ? typeElement.GetString()! : "unknown";
        string reason = cause.TryGetProperty("reason", out var reasonElement) && reasonElement.ValueKind is JsonValueKind.String ? reasonElement.GetString()! : String.Empty;
        return id is null ? $"{type} - {reason}" : $"{id}: {type} - {reason}";
    }
}
