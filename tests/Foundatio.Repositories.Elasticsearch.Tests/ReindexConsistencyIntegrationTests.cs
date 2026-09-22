using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Elastic.Transport;
using Foundatio.Repositories.Elasticsearch.Jobs;
using Foundatio.Utility;
using Xunit;
using HttpMethod = Elastic.Transport.HttpMethod;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class ReindexConsistencyIntegrationTests : ElasticRepositoryTestBase
{
    public ReindexConsistencyIntegrationTests(ITestOutputHelper output) : base(output) { }

    [Fact]
    public async Task RoutedDuplicates_DeleteOnlyTheMissingRoutingAndKeepRetiredSourceFenced()
    {
        var item = await CreateFixtureAsync(true);
        await using var cleanup = new AsyncDisposableAction(() => CleanupAsync(item));
        var (first, second) = await GetDistinctRoutesAsync(item.OldIndex);
        await RawAsync(HttpMethod.PUT, $"/{item.OldIndex}/_doc/shared?routing={first}&refresh=true", new { value = "first" });
        await RawAsync(HttpMethod.PUT, $"/{item.OldIndex}/_doc/shared?routing={second}&refresh=true", new { value = "second" });
        bool deleted = false;
        var reindexer = new ElasticReindexer(_client, _configuration.Serializer);
        await reindexer.ReindexAsync(item, async (progress, _) =>
        {
            if (progress is 91 && !deleted)
            {
                deleted = true;
                await RawAsync(HttpMethod.DELETE, $"/{item.OldIndex}/_doc/shared?routing={first}&refresh=true");
            }
        }, TestCancellationToken);
        Assert.True(deleted);
        Assert.Equal(404, (await RawAsync(HttpMethod.GET, $"/{item.NewIndex}/_doc/shared?routing={first}", allowFailure: true)).ApiCallDetails.HttpStatusCode);
        using var survivor = JsonDocument.Parse((await RawAsync(HttpMethod.GET, $"/{item.NewIndex}/_doc/shared?routing={second}")).Body!);
        Assert.Equal("second", survivor.RootElement.GetProperty("_source").GetProperty("value").GetString());
        Assert.Equal(403, (await RawAsync(HttpMethod.PUT, $"/{item.OldIndex}/_doc/stale?routing={first}", new { value = "stale" }, true)).ApiCallDetails.HttpStatusCode);
        await RawAsync(HttpMethod.PUT, $"/{item.Alias}/_doc/new?routing={second}", new { value = "new" });
    }

    [Fact]
    public async Task DeletePlusInsert_DoesNotUseEqualCountsAsIdentityProof()
    {
        var item = await CreateFixtureAsync(false);
        await using var cleanup = new AsyncDisposableAction(() => CleanupAsync(item));
        await RawAsync(HttpMethod.PUT, $"/{item.OldIndex}/_doc/a?refresh=true", new { value = "a" });
        await RawAsync(HttpMethod.PUT, $"/{item.OldIndex}/_doc/b?refresh=true", new { value = "b" });
        bool changed = false;
        var reindexer = new ElasticReindexer(_client, _configuration.Serializer);
        await reindexer.ReindexAsync(item, async (progress, _) =>
        {
            if (progress is 91 && !changed)
            {
                changed = true;
                await RawAsync(HttpMethod.DELETE, $"/{item.OldIndex}/_doc/b?refresh=true");
                await RawAsync(HttpMethod.PUT, $"/{item.OldIndex}/_doc/c?refresh=true", new { value = "c" });
            }
        }, TestCancellationToken);
        Assert.True(changed);
        Assert.Equal(404, (await RawAsync(HttpMethod.GET, $"/{item.NewIndex}/_doc/b", allowFailure: true)).ApiCallDetails.HttpStatusCode);
        await RawAsync(HttpMethod.GET, $"/{item.NewIndex}/_doc/a");
        await RawAsync(HttpMethod.GET, $"/{item.NewIndex}/_doc/c");
    }

    [Fact]
    public async Task PrimaryCheckpointVector_DetectsAQuietShardUpdate()
    {
        var item = await CreateFixtureAsync(true);
        await using var cleanup = new AsyncDisposableAction(() => CleanupAsync(item));
        var (hot, quiet) = await GetDistinctRoutesAsync(item.OldIndex);
        for (int i = 0; i < 30; i++)
            await RawAsync(HttpMethod.PUT, $"/{item.OldIndex}/_doc/hot?routing={hot}", new { value = i });
        await RawAsync(HttpMethod.PUT, $"/{item.OldIndex}/_doc/quiet?routing={quiet}", new { value = 1 });
        var before = await ReindexSourceCheckpoint.ReadAsync(_client, item.OldIndex, TestCancellationToken);
        await RawAsync(HttpMethod.PUT, $"/{item.OldIndex}/_doc/quiet?routing={quiet}", new { value = 2 });
        var after = await ReindexSourceCheckpoint.ReadAsync(_client, item.OldIndex, TestCancellationToken);
        Assert.Equal(before.Primaries.Values.Max(), after.Primaries.Values.Max());
        Assert.False(before.Matches(after));
    }

    private async Task<ReindexWorkItem> CreateFixtureAsync(bool routed)
    {
        string alias = $"consistency-{Guid.NewGuid():N}";
        var item = new ReindexWorkItem { OldIndex = $"{alias}-v1", NewIndex = $"{alias}-v2", Alias = alias, QuiesceSource = true, DeleteOld = false };
        await RawAsync(HttpMethod.PUT, $"/{item.OldIndex}", new
        {
            settings = new { number_of_shards = 2, number_of_replicas = 0 },
            mappings = new { _routing = new { required = routed } },
            aliases = new Dictionary<string, object> { [alias] = new { } }
        });
        await RawAsync(HttpMethod.PUT, $"/{item.NewIndex}", new
        {
            settings = new { number_of_shards = 2, number_of_replicas = 0 },
            mappings = new { _routing = new { required = routed } }
        });
        return item;
    }

    private async Task<(string First, string Second)> GetDistinctRoutesAsync(string index)
    {
        string? first = null;
        int? firstShard = null;
        for (int i = 0; i < 100; i++)
        {
            string route = $"tenant-{i}";
            using var response = JsonDocument.Parse((await RawAsync(HttpMethod.GET, $"/{index}/_search_shards?routing={route}")).Body!);
            int shard = response.RootElement.GetProperty("shards")[0][0].GetProperty("shard").GetInt32();
            if (firstShard is null)
            {
                first = route;
                firstShard = shard;
            }
            else if (shard != firstShard)
                return (first!, route);
        }
        throw new InvalidOperationException("Could not route the test documents to different shards.");
    }

    private Task CleanupAsync(ReindexWorkItem item)
    {
        return RawAsync(HttpMethod.DELETE, $"/{item.OldIndex},{item.NewIndex}?ignore_unavailable=true");
    }

    private async Task<StringResponse> RawAsync(HttpMethod method, string path, object? body = null, bool allowFailure = false)
    {
        var response = await _client.Transport.RequestAsync<StringResponse>(method, path,
            body is null ? null : PostData.String(JsonSerializer.Serialize(body)), TestCancellationToken);
        if (!allowFailure)
            Assert.True(response.ApiCallDetails.HasSuccessfulStatusCode, response.Body);
        return response;
    }
}
