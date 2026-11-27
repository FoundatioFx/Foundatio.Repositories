using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Transport.Extensions;
using Foundatio.Repositories.Elasticsearch.Extensions;

namespace Foundatio.Repositories.Elasticsearch.Configuration;

internal static class CompatibilityCutoverReceipt
{
    private const string Prefix = ".foundatio-cutover-v1-";

    public static bool IsReserved(string name) => name.StartsWith(Prefix, StringComparison.Ordinal);

    // Written in the same cluster-state update that removes the source. The source UUID fingerprint
    // survives its deletion, so a restarted process can validate the original cutover manifest.
    public static string Create(ElasticsearchClient client, string owner, string source, string sourceUuid,
        string target, string targetUuid, IReadOnlyDictionary<string, Alias> aliases)
    {
        ArgumentException.ThrowIfNullOrEmpty(sourceUuid);
        ArgumentException.ThrowIfNullOrEmpty(targetUuid);
        string attempt = Guid.NewGuid().ToString("N");
        string sourceFingerprint = Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(sourceUuid)));
        return Build(client, attempt, sourceFingerprint, owner, source, target, targetUuid, aliases);
    }

    public static string? Find(ElasticsearchClient client, string owner, string source, string target,
        string? targetUuid, IReadOnlyDictionary<string, Alias> aliases)
    {
        if (String.IsNullOrEmpty(targetUuid))
            return null;

        var receipts = aliases.Keys.Where(IsReserved).Take(2).ToArray();
        if (receipts.Length is not 1 || !aliases.HasExactHiddenAlias(receipts[0]))
            return null;

        string[] parts = receipts[0][Prefix.Length..].Split('.');
        if (parts.Length is not 3 || !Guid.TryParseExact(parts[0], "N", out _)
            || parts[1].Length is not 64 || !parts[1].All(Char.IsAsciiHexDigit))
            return null;

        string expected = Build(client, parts[0], parts[1], owner, source, target, targetUuid, aliases);
        return String.Equals(expected, receipts[0], StringComparison.Ordinal) ? expected : null;
    }

    private static string Build(ElasticsearchClient client, string attempt, string sourceFingerprint, string owner,
        string source, string target, string targetUuid, IReadOnlyDictionary<string, Alias> aliases)
    {
        using var manifest = new MemoryStream();
        using (var writer = new Utf8JsonWriter(manifest))
        {
            writer.WriteStartArray();
            foreach (string field in new[] { attempt, sourceFingerprint, owner, source, target, targetUuid })
                writer.WriteStringValue(field);
            writer.WriteStartObject();
            foreach (var alias in aliases.Where(pair => !IsReserved(pair.Key)).OrderBy(pair => pair.Key, StringComparer.Ordinal))
            {
                writer.WritePropertyName(alias.Key);
                using var definition = JsonDocument.Parse(client.ElasticsearchClientSettings.RequestResponseSerializer.SerializeToString(alias.Value));
                WriteCanonical(writer, definition.RootElement);
            }
            writer.WriteEndObject();
            writer.WriteEndArray();
        }
        return $"{Prefix}{attempt}.{sourceFingerprint}.{Convert.ToHexStringLower(SHA256.HashData(manifest.ToArray()))}";
    }

    private static void WriteCanonical(Utf8JsonWriter writer, JsonElement value)
    {
        if (value.ValueKind is JsonValueKind.Object)
        {
            writer.WriteStartObject();
            foreach (var property in value.EnumerateObject().OrderBy(property => property.Name, StringComparer.Ordinal))
            {
                writer.WritePropertyName(property.Name);
                WriteCanonical(writer, property.Value);
            }
            writer.WriteEndObject();
        }
        else if (value.ValueKind is JsonValueKind.Array)
        {
            writer.WriteStartArray();
            foreach (var item in value.EnumerateArray())
                WriteCanonical(writer, item);
            writer.WriteEndArray();
        }
        else
            value.WriteTo(writer);
    }
}
