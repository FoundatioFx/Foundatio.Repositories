from pathlib import Path
import sys
root = Path(sys.argv[1])
p = root / 'src/Foundatio.Repositories.Elasticsearch/Repositories/ReindexEvidence.cs'
assert not p.exists()
p.write_text('''using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Extensions;

namespace Foundatio.Repositories.Elasticsearch;

internal readonly record struct ReindexDocumentIdentity(string Id, string? Routing);

internal static class ReindexEvidence
{
    public static void RequireCompleteSearch(JsonElement root)
    {
        if (root.ValueKind is not JsonValueKind.Object
            || !root.TryGetProperty("timed_out", out var timedOut) || timedOut.ValueKind is not JsonValueKind.False
            || !root.TryGetProperty("_shards", out var shards)
            || !TryInteger(shards, "total", out long total) || total <= 0
            || !TryInteger(shards, "successful", out long successful) || successful != total
            || !TryInteger(shards, "failed", out long failed) || failed is not 0)
            throw new RepositoryException("Reconciliation search did not provide complete, successful shard and timeout evidence.");
    }

    public static (List<ReindexDocumentIdentity> Ids, string ScrollId) ReadPage(string body, string index)
    {
        using var document = JsonDocument.Parse(body);
        var root = document.RootElement;
        RequireCompleteSearch(root);
        string scrollId = RequiredString(root, "_scroll_id");
        if (!root.TryGetProperty("hits", out var hits) || hits.ValueKind is not JsonValueKind.Object
            || !hits.TryGetProperty("hits", out var rows) || rows.ValueKind is not JsonValueKind.Array)
            throw new RepositoryException("Reconciliation search omitted its result page; this is not end-of-data evidence.");

        var ids = new List<ReindexDocumentIdentity>(rows.GetArrayLength());
        foreach (var hit in rows.EnumerateArray())
        {
            if (!String.Equals(RequiredString(hit, "_index"), index, StringComparison.Ordinal))
                throw new RepositoryException("Reconciliation search resolved a different physical index.");
            string? routing = null;
            if (hit.TryGetProperty("_routing", out var route))
                routing = RequiredString(hit, "_routing");
            else if (hit.TryGetProperty("fields", out var fields) && fields.ValueKind is JsonValueKind.Object
                && fields.TryGetProperty("_routing", out route))
            {
                if (route.ValueKind is not JsonValueKind.Array || route.GetArrayLength() is not 1
                    || route[0].ValueKind is not JsonValueKind.String || String.IsNullOrEmpty(route[0].GetString()))
                    throw new RepositoryException("Reconciliation search returned malformed document routing.");
                routing = route[0].GetString();
            }
            ids.Add(new ReindexDocumentIdentity(RequiredString(hit, "_id"), routing));
        }
        return (ids, scrollId);
    }

    public static List<ReindexDocumentIdentity> ReadMissing(string body, string index, IReadOnlyList<ReindexDocumentIdentity> requested)
    {
        using var document = JsonDocument.Parse(body);
        var root = document.RootElement;
        if (root.ValueKind is not JsonValueKind.Object || !root.TryGetProperty("docs", out var docs)
            || docs.ValueKind is not JsonValueKind.Array || docs.GetArrayLength() != requested.Count)
            throw new RepositoryException("Reconciliation multi-get did not account for every requested document.");

        var missing = new List<ReindexDocumentIdentity>();
        for (int i = 0; i < requested.Count; i++)
        {
            var item = docs[i];
            if (!String.Equals(RequiredString(item, "_index"), index, StringComparison.Ordinal)
                || !String.Equals(RequiredString(item, "_id"), requested[i].Id, StringComparison.Ordinal)
                || item.TryGetProperty("error", out _)
                || !item.TryGetProperty("found", out var found)
                || found.ValueKind is not (JsonValueKind.True or JsonValueKind.False))
                throw new RepositoryException("Reconciliation multi-get returned a failed, malformed or mismatched item; failure is not document absence.");

            if (item.TryGetProperty("_routing", out var route)
                && (route.ValueKind is not JsonValueKind.String || !String.Equals(route.GetString(), requested[i].Routing, StringComparison.Ordinal)))
                throw new RepositoryException("Reconciliation multi-get returned mismatched routing.");
            if (found.ValueKind is JsonValueKind.False)
                missing.Add(requested[i]);
        }
        return missing;
    }

    public static void RequireDeletes(string body, string index, IReadOnlyList<ReindexDocumentIdentity> requested)
    {
        using var document = JsonDocument.Parse(body);
        var root = document.RootElement;
        if (root.ValueKind is not JsonValueKind.Object || !root.TryGetProperty("errors", out var errors)
            || errors.ValueKind is not JsonValueKind.False || !root.TryGetProperty("items", out var items)
            || items.ValueKind is not JsonValueKind.Array || items.GetArrayLength() != requested.Count)
            throw new RepositoryException("Reconciliation bulk delete was incomplete or contained item failures.");

        for (int i = 0; i < requested.Count; i++)
        {
            var wrapper = items[i];
            if (wrapper.ValueKind is not JsonValueKind.Object || !wrapper.TryGetProperty("delete", out var item)
                || !String.Equals(RequiredString(item, "_index"), index, StringComparison.Ordinal)
                || !String.Equals(RequiredString(item, "_id"), requested[i].Id, StringComparison.Ordinal)
                || item.TryGetProperty("error", out _) || !TryInteger(item, "status", out long status)
                || !item.TryGetProperty("result", out var result) || result.ValueKind is not JsonValueKind.String
                || !((status is 200 && result.GetString() is "deleted") || (status is 404 && result.GetString() is "not_found")))
                throw new RepositoryException("Reconciliation bulk delete did not positively account for a requested identity.");
        }
    }

    internal static string RequiredString(JsonElement root, string property)
    {
        if (root.ValueKind is not JsonValueKind.Object || !root.TryGetProperty(property, out var value)
            || value.ValueKind is not JsonValueKind.String || String.IsNullOrEmpty(value.GetString()))
            throw new RepositoryException($"Reconciliation evidence omitted a valid '{property}'.");
        return value.GetString()!;
    }

    internal static bool TryInteger(JsonElement root, string property, out long value)
    {
        value = 0;
        return root.ValueKind is JsonValueKind.Object && root.TryGetProperty(property, out var item)
            && item.ValueKind is JsonValueKind.Number && item.TryGetInt64(out value);
    }
}

internal sealed record ReindexSourceCheckpoint(string Uuid, IReadOnlyDictionary<int, long> Primaries)
{
    public bool Matches(ReindexSourceCheckpoint other) => String.Equals(Uuid, other.Uuid, StringComparison.Ordinal)
        && Primaries.Count == other.Primaries.Count
        && Primaries.All(p => other.Primaries.TryGetValue(p.Key, out long value) && value == p.Value);

    public static async Task<ReindexSourceCheckpoint> ReadAsync(ElasticsearchClient client, string index, CancellationToken cancellationToken)
    {
        string escaped = Uri.EscapeDataString(index);
        var settings = await client.Transport.RequestAsync<StringResponse>(Elastic.Transport.HttpMethod.GET,
            $"/{escaped}/_settings?flat_settings=true", cancellationToken).AnyContext();
        if (settings.ApiCallDetails?.HasSuccessfulStatusCode is not true || String.IsNullOrEmpty(settings.Body))
            throw new RepositoryException($"Unable to establish the generation and primary-shard count of '{index}'.");
        var stats = await client.Transport.RequestAsync<StringResponse>(Elastic.Transport.HttpMethod.GET,
            $"/{escaped}/_stats?level=shards", cancellationToken).AnyContext();
        if (stats.ApiCallDetails?.HasSuccessfulStatusCode is not true || String.IsNullOrEmpty(stats.Body))
            throw new RepositoryException($"Unable to establish all primary-shard checkpoints of '{index}'.");
        return Parse(index, settings.Body, stats.Body);
    }

    internal static ReindexSourceCheckpoint Parse(string index, string settingsBody, string statsBody)
    {
        using var settingsDocument = JsonDocument.Parse(settingsBody);
        var root = settingsDocument.RootElement;
        if (root.ValueKind is not JsonValueKind.Object || root.EnumerateObject().Count() is not 1
            || !root.TryGetProperty(index, out var entry) || entry.ValueKind is not JsonValueKind.Object
            || !entry.TryGetProperty("settings", out var settings))
            throw new RepositoryException("Checkpoint settings did not resolve one exact physical index.");
        string uuid = ReindexEvidence.RequiredString(settings, "index.uuid");
        string shardCount = ReindexEvidence.RequiredString(settings, "index.number_of_shards");
        if (!Int32.TryParse(shardCount, NumberStyles.None, CultureInfo.InvariantCulture, out int count) || count <= 0)
            throw new RepositoryException("Checkpoint settings omitted a valid primary-shard count.");

        using var statsDocument = JsonDocument.Parse(statsBody);
        root = statsDocument.RootElement;
        if (root.ValueKind is not JsonValueKind.Object || !root.TryGetProperty("_shards", out var summary)
            || !ReindexEvidence.TryInteger(summary, "failed", out long failed) || failed is not 0
            || !root.TryGetProperty("indices", out var indices) || indices.ValueKind is not JsonValueKind.Object
            || indices.EnumerateObject().Count() is not 1 || !indices.TryGetProperty(index, out entry)
            || !String.Equals(ReindexEvidence.RequiredString(entry, "uuid"), uuid, StringComparison.Ordinal)
            || !entry.TryGetProperty("shards", out var shards) || shards.ValueKind is not JsonValueKind.Object
            || shards.EnumerateObject().Count() != count)
            throw new RepositoryException("Checkpoint statistics were partial or belonged to a different physical generation.");

        var primaries = new Dictionary<int, long>(count);
        for (int i = 0; i < count; i++)
        {
            if (!shards.TryGetProperty(i.ToString(CultureInfo.InvariantCulture), out var copies) || copies.ValueKind is not JsonValueKind.Array)
                throw new RepositoryException("Checkpoint statistics omitted an expected primary shard.");
            int primaryCount = 0;
            foreach (var copy in copies.EnumerateArray())
            {
                if (!copy.TryGetProperty("routing", out var routing) || routing.ValueKind is not JsonValueKind.Object
                    || !routing.TryGetProperty("primary", out var primary) || primary.ValueKind is not JsonValueKind.True)
                    continue;
                primaryCount++;
                if (!copy.TryGetProperty("seq_no", out var sequence)
                    || !ReindexEvidence.TryInteger(sequence, "max_seq_no", out long maximum) || maximum < -1)
                    throw new RepositoryException("Checkpoint statistics omitted a primary-shard sequence number.");
                primaries[i] = maximum;
            }
            if (primaryCount is not 1)
                throw new RepositoryException("Checkpoint statistics did not identify exactly one copy of each primary shard.");
        }
        return new ReindexSourceCheckpoint(uuid, primaries);
    }
}
''')
