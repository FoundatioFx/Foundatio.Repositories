using System;
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

/// <summary>A complete, generation-bound observation of every primary shard, not an index-wide maximum.</summary>
internal sealed class ReindexSourceCheckpoint
{
    public string IndexUuid { get; }
    public IReadOnlyDictionary<int, ShardCheckpoint> Shards { get; }

    private ReindexSourceCheckpoint(string indexUuid, Dictionary<int, ShardCheckpoint> shards)
    {
        IndexUuid = indexUuid;
        Shards = shards;
    }

    public bool Matches(ReindexSourceCheckpoint other)
        => String.Equals(IndexUuid, other.IndexUuid, StringComparison.Ordinal)
            && Shards.Count == other.Shards.Count
            && Shards.All(s => other.Shards.TryGetValue(s.Key, out var value) && value == s.Value);

    public static async Task<ReindexSourceCheckpoint> ReadAsync(ElasticsearchClient client, string index, CancellationToken cancellationToken)
    {
        string path = Uri.EscapeDataString(index);
        var settingsResponse = await client.Transport.RequestAsync<StringResponse>(Elastic.Transport.HttpMethod.GET, $"/{path}/_settings", cancellationToken).AnyContext();
        using var settings = ReindexResponse.Parse(settingsResponse, "Reading source settings", cancellationToken);
        var statsResponse = await client.Transport.RequestAsync<StringResponse>(Elastic.Transport.HttpMethod.GET, $"/{path}/_stats?level=shards", cancellationToken).AnyContext();
        using var stats = ReindexResponse.Parse(statsResponse, "Reading source shard checkpoints", cancellationToken);
        return Parse(index, settings.RootElement, stats.RootElement);
    }

    internal static ReindexSourceCheckpoint Parse(string index, JsonElement settings, JsonElement stats)
    {
        var indexSettings = ReindexResponse.Required(ReindexResponse.Required(ReindexResponse.Required(settings, index, JsonValueKind.Object), "settings", JsonValueKind.Object), "index", JsonValueKind.Object);
        string uuid = ReindexResponse.Text(indexSettings, "uuid");
        if (String.IsNullOrEmpty(uuid) || !indexSettings.TryGetProperty("number_of_shards", out var count)
            || !Int32.TryParse(count.ToString(), NumberStyles.None, CultureInfo.InvariantCulture, out int expectedShards) || expectedShards <= 0)
            throw new RepositoryException("Source settings did not identify its UUID and primary shard count.");

        var summary = ReindexResponse.Required(stats, "_shards", JsonValueKind.Object);
        if (ReindexResponse.Number(summary, "failed") is not 0)
            throw new RepositoryException("Source shard statistics are incomplete.");
        var indices = ReindexResponse.Required(stats, "indices", JsonValueKind.Object);
        var indexStats = ReindexResponse.Required(indices, index, JsonValueKind.Object);
        if (!String.Equals(ReindexResponse.Text(indexStats, "uuid"), uuid, StringComparison.Ordinal))
            throw new RepositoryException("Source index was recreated while reading checkpoints.");
        var shardStats = ReindexResponse.Required(indexStats, "shards", JsonValueKind.Object);
        var result = new Dictionary<int, ShardCheckpoint>();
        foreach (var property in shardStats.EnumerateObject())
        {
            if (!Int32.TryParse(property.Name, NumberStyles.None, CultureInfo.InvariantCulture, out int shardId)
                || shardId < 0 || shardId >= expectedShards || property.Value.ValueKind is not JsonValueKind.Array)
                throw new RepositoryException("Source shard statistics contain an invalid shard identity.");
            foreach (var copy in property.Value.EnumerateArray())
            {
                var routing = ReindexResponse.Required(copy, "routing", JsonValueKind.Object);
                if (!routing.TryGetProperty("primary", out var primary) || primary.ValueKind is not (JsonValueKind.True or JsonValueKind.False))
                    throw new RepositoryException("Source statistics did not identify the primary shard.");
                if (primary.ValueKind is JsonValueKind.False)
                    continue;
                var sequence = ReindexResponse.Required(copy, "seq_no", JsonValueKind.Object);
                long maximum = ReindexResponse.Number(sequence, "max_seq_no");
                long local = ReindexResponse.Number(sequence, "local_checkpoint");
                if (maximum < -1 || local < -1 || local > maximum || !result.TryAdd(shardId, new ShardCheckpoint(maximum, local)))
                    throw new RepositoryException("Source statistics contain invalid or duplicate primary checkpoints.");
            }
        }
        if (result.Count != expectedShards)
            throw new RepositoryException("Source statistics omitted at least one primary shard.");
        return new ReindexSourceCheckpoint(uuid, result);
    }

    internal readonly record struct ShardCheckpoint(long Maximum, long Local);
}
