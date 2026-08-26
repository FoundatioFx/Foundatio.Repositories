using System.Text.Json;

namespace Foundatio.Repositories.Elasticsearch;

internal static class ElasticTaskResponseParser
{
    private static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web);

    public static T? Deserialize<T>(object? response)
    {
        if (response is null)
            return default;

        var element = response is JsonElement jsonElement
            ? jsonElement
            : JsonSerializer.SerializeToElement(response, Options);

        return element.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined
            ? default
            : element.Deserialize<T>(Options);
    }

    /// <summary>
    /// Returns whether an Elasticsearch task listing or cancellation response reported partial failures. A
    /// non-array value or any array entry means the response omitted results, so task state is unknown.
    /// </summary>
    public static bool HasPartialTaskListFailures(JsonElement response)
    {
        if (response.ValueKind is not JsonValueKind.Object)
            return true;

        return HasFailureCollection(response, "node_failures") || HasFailureCollection(response, "task_failures");
    }

    private static bool HasFailureCollection(JsonElement response, string propertyName)
    {
        if (!response.TryGetProperty(propertyName, out var failures))
            return false;

        return failures.ValueKind is not JsonValueKind.Array || failures.GetArrayLength() > 0;
    }
}
