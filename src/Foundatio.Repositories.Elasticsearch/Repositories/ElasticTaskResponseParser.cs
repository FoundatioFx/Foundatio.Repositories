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
}
