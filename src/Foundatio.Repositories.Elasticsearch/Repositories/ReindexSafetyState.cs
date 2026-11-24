using System;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Extensions;

namespace Foundatio.Repositories.Elasticsearch;

/// <summary>Durable intent for side effects that outlive a reindex process. Callers must hold the alias lock.</summary>
internal static class ReindexSafetyState
{
    public const string Index = "foundatio-reindex-safety";

    internal sealed record Entry(string Kind, string Source, string Destination, string Alias, string SourceUuid, string? TaskId = null, string? Phase = null)
    {
        public string OwnerToken { get; init; } = Guid.NewGuid().ToString("N");
    }

    public static string GetId(string kind, string identity)
        => kind + "-" + Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(identity))).ToLowerInvariant();

    public static async Task<Entry?> ReadAsync(ElasticsearchClient client, string id, CancellationToken cancellationToken)
    {
        var response = await client.Transport.RequestAsync<StringResponse>(Elastic.Transport.HttpMethod.GET, $"/{Index}/_doc/{id}", cancellationToken).AnyContext();
        cancellationToken.ThrowIfCancellationRequested();
        if (response.ApiCallDetails?.HttpStatusCode is 404)
            return null;
        using var document = ReindexResponse.Parse(response, "Reading durable reindex safety state", cancellationToken);
        var source = ReindexResponse.Required(document.RootElement, "_source", JsonValueKind.Object);
        if (String.IsNullOrEmpty(ReindexResponse.Text(source, "OwnerToken")))
            throw new RepositoryException("Reindex safety state has no ownership token.");
        var entry = JsonSerializer.Deserialize<Entry>(source.GetRawText());
        if (entry is null || String.IsNullOrEmpty(entry.Kind) || String.IsNullOrEmpty(entry.Source)
            || String.IsNullOrEmpty(entry.Destination) || String.IsNullOrEmpty(entry.Alias) || entry.SourceUuid is null)
            throw new RepositoryException("Reindex safety state could not be read.");
        return entry;
    }

    public static async Task WriteAsync(ElasticsearchClient client, string id, Entry entry, bool createOnly, CancellationToken cancellationToken)
    {
        // Stored, not searched: a closed mapping avoids consumer field-name conventions affecting recovery.
        var create = await client.Transport.RequestAsync<StringResponse>(Elastic.Transport.HttpMethod.PUT, $"/{Index}",
            PostData.String("""{"settings":{"number_of_shards":1},"mappings":{"dynamic":false}}"""), cancellationToken).AnyContext();
        cancellationToken.ThrowIfCancellationRequested();
        if (create.ApiCallDetails?.HasSuccessfulStatusCode is not true)
        {
            using var error = JsonDocument.Parse(create.Body ?? "{}");
            if (create.ApiCallDetails?.HttpStatusCode is not 400
                || !error.RootElement.TryGetProperty("error", out var detail)
                || detail.ValueKind is not JsonValueKind.Object
                || !detail.TryGetProperty("type", out var type)
                || type.GetString() is not "resource_already_exists_exception")
                throw new RepositoryException("Could not provision durable reindex safety state.");
        }
        string query = createOnly ? "?op_type=create" : await GetOwnedVersionQueryAsync(client, id, entry.OwnerToken, cancellationToken).AnyContext();
        var response = await client.Transport.RequestAsync<StringResponse>(Elastic.Transport.HttpMethod.PUT, $"/{Index}/_doc/{id}{query}",
            PostData.String(JsonSerializer.Serialize(entry)), cancellationToken).AnyContext();
        using var document = ReindexResponse.Parse(response, "Writing durable reindex safety state", cancellationToken);
        string result = ReindexResponse.Text(document.RootElement, "result");
        if (result is not ("created" or "updated"))
            throw new RepositoryException("Reindex safety state was not persisted.");
    }

    private static async Task<string> GetOwnedVersionQueryAsync(ElasticsearchClient client, string id, string ownerToken, CancellationToken cancellationToken)
    {
        var response = await client.Transport.RequestAsync<StringResponse>(Elastic.Transport.HttpMethod.GET, $"/{Index}/_doc/{id}", cancellationToken).AnyContext();
        using var document = ReindexResponse.Parse(response, "Verifying reindex safety ownership", cancellationToken);
        var root = document.RootElement;
        var source = ReindexResponse.Required(root, "_source", JsonValueKind.Object);
        if (!String.Equals(ReindexResponse.Text(source, "OwnerToken"), ownerToken, StringComparison.Ordinal))
            throw new RepositoryException("Reindex safety ownership changed; a stale worker must not overwrite or delete the newer attempt's record.");
        long sequence = ReindexResponse.Number(root, "_seq_no");
        long primaryTerm = ReindexResponse.Number(root, "_primary_term");
        if (sequence < 0 || primaryTerm <= 0)
            throw new RepositoryException("Reindex safety record version is invalid.");
        return $"?if_seq_no={sequence}&if_primary_term={primaryTerm}";
    }

    public static async Task<string> ReadIndexUuidAsync(ElasticsearchClient client, string index, CancellationToken cancellationToken)
    {
        var response = await client.Transport.RequestAsync<StringResponse>(Elastic.Transport.HttpMethod.GET,
            $"/{Uri.EscapeDataString(index)}/_settings", cancellationToken).AnyContext();
        using var document = ReindexResponse.Parse(response, "Reading index generation for reindex ownership", cancellationToken);
        var settings = ReindexResponse.Required(ReindexResponse.Required(ReindexResponse.Required(document.RootElement,
            index, JsonValueKind.Object), "settings", JsonValueKind.Object), "index", JsonValueKind.Object);
        string uuid = ReindexResponse.Text(settings, "uuid");
        if (String.IsNullOrEmpty(uuid))
            throw new RepositoryException("Index generation is unknown; refusing reindex ownership recovery.");
        return uuid;
    }

    public static async Task DeleteAsync(ElasticsearchClient client, string id, string ownerToken, CancellationToken cancellationToken)
    {
        string query = await GetOwnedVersionQueryAsync(client, id, ownerToken, cancellationToken).AnyContext();
        var response = await client.Transport.RequestAsync<StringResponse>(Elastic.Transport.HttpMethod.DELETE, $"/{Index}/_doc/{id}{query}", cancellationToken).AnyContext();
        cancellationToken.ThrowIfCancellationRequested();
        if (response.ApiCallDetails?.HttpStatusCode is 404)
            return;
        using var document = ReindexResponse.Parse(response, "Clearing durable reindex safety state", cancellationToken);
        if (ReindexResponse.Text(document.RootElement, "result") is not "deleted")
            throw new RepositoryException("Reindex safety state deletion was not confirmed.");
    }
}
