using System;
using System.Text.Json;
using System.Threading;
using Elastic.Transport;
using Foundatio.Repositories.Exceptions;

namespace Foundatio.Repositories.Elasticsearch;

/// <summary>Rejects incomplete transport evidence before it can authorize a migration mutation.</summary>
internal static class ReindexResponse
{
    public static JsonDocument Parse(StringResponse response, string operation, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (response.ApiCallDetails?.HasSuccessfulStatusCode is not true || String.IsNullOrEmpty(response.Body))
            throw new RepositoryException($"{operation} failed or returned an empty response.");
        try
        {
            var document = JsonDocument.Parse(response.Body);
            if (document.RootElement.ValueKind is JsonValueKind.Object)
                return document;
            document.Dispose();
            throw new RepositoryException($"{operation} did not return an object.");
        }
        catch (JsonException ex)
        {
            throw new RepositoryException($"{operation} returned invalid JSON.", ex);
        }
    }

    public static JsonElement Required(JsonElement parent, string name, JsonValueKind kind)
    {
        if (parent.ValueKind is not JsonValueKind.Object || !parent.TryGetProperty(name, out var value) || value.ValueKind != kind)
            throw new RepositoryException($"Migration response is missing a valid {name} field.");
        return value;
    }

    public static long Number(JsonElement parent, string name)
    {
        var value = Required(parent, name, JsonValueKind.Number);
        if (!value.TryGetInt64(out long result))
            throw new RepositoryException($"Migration response contains an invalid {name} counter.");
        return result;
    }

    public static string Text(JsonElement parent, string name)
        => Required(parent, name, JsonValueKind.String).GetString()!;

    public static void RequireCompleteSearch(JsonElement root)
    {
        Required(root, "timed_out", JsonValueKind.False);
        var shards = Required(root, "_shards", JsonValueKind.Object);
        if (Number(shards, "failed") is not 0 || Number(shards, "successful") != Number(shards, "total") || Number(shards, "total") <= 0)
            throw new RepositoryException("Migration search returned incomplete shard results.");
    }
}
