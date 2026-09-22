using System;
using System.Collections.Generic;
using System.Text.Json;
using Foundatio.Repositories.Exceptions;

namespace Foundatio.Repositories.Elasticsearch;

/// <summary>Validates per-document evidence used by delete reconciliation.</summary>
internal static class ReindexDocumentBatch
{
    internal readonly record struct Key(string Id, string? Routing);

    public static Key ReadKey(JsonElement hit)
    {
        string id = ReindexResponse.Text(hit, "_id");
        string? routing = null;
        if (hit.TryGetProperty("_routing", out var directRouting))
        {
            if (directRouting.ValueKind is not JsonValueKind.String)
                throw new RepositoryException("Invalid document routing in migration search.");
            routing = directRouting.GetString();
        }
        else if (hit.TryGetProperty("fields", out var fields))
        {
            if (fields.ValueKind is not JsonValueKind.Object)
                throw new RepositoryException("Invalid stored fields in migration search.");
            if (fields.TryGetProperty("_routing", out var values))
            {
                if (values.ValueKind is not JsonValueKind.Array || values.GetArrayLength() is not 1 || values[0].ValueKind is not JsonValueKind.String)
                    throw new RepositoryException("Invalid stored document routing in migration search.");
                routing = values[0].GetString();
            }
        }
        return new Key(id, routing);
    }

    public static List<Key> ReadMissing(string index, IReadOnlyList<Key> requested, JsonElement root)
    {
        var docs = ReindexResponse.Required(root, "docs", JsonValueKind.Array);
        if (docs.GetArrayLength() != requested.Count)
            throw new RepositoryException("Multi-get omitted document results during delete reconciliation.");
        var missing = new List<Key>();
        for (int i = 0; i < requested.Count; i++)
        {
            var doc = docs[i];
            var key = requested[i];
            if (doc.TryGetProperty("error", out _) || !String.Equals(ReindexResponse.Text(doc, "_index"), index, StringComparison.Ordinal)
                || !String.Equals(ReindexResponse.Text(doc, "_id"), key.Id, StringComparison.Ordinal)
                || !doc.TryGetProperty("found", out var found) || found.ValueKind is not (JsonValueKind.True or JsonValueKind.False))
                throw new RepositoryException("Multi-get did not establish source document presence; refusing to infer deletion.");
            // Routing is part of the document identity. A same-id document found on a colliding shard with
            // different routing does not vouch for the destination's old routing value.
            if (found.ValueKind is JsonValueKind.False || ReadKey(doc).Routing != key.Routing)
                missing.Add(key);
        }
        return missing;
    }

    public static void RequireSuccessfulDeletes(string index, IReadOnlyList<Key> requested, JsonElement root)
    {
        ReindexResponse.Required(root, "errors", JsonValueKind.False);
        var items = ReindexResponse.Required(root, "items", JsonValueKind.Array);
        if (items.GetArrayLength() != requested.Count)
            throw new RepositoryException("Bulk deletion omitted per-document results.");
        for (int i = 0; i < requested.Count; i++)
        {
            var item = ReindexResponse.Required(items[i], "delete", JsonValueKind.Object);
            long status = ReindexResponse.Number(item, "status");
            string result = ReindexResponse.Text(item, "result");
            if (item.TryGetProperty("error", out _) || !String.Equals(ReindexResponse.Text(item, "_index"), index, StringComparison.Ordinal)
                || !String.Equals(ReindexResponse.Text(item, "_id"), requested[i].Id, StringComparison.Ordinal)
                || !(status is 200 && result is "deleted" || status is 404 && result is "not_found"))
                throw new RepositoryException("Bulk deletion failed for at least one document; refusing promotion.");
        }
    }
}
