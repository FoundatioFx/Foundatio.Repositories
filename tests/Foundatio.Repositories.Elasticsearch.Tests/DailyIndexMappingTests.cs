using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Mapping;
using Elastic.Transport;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Xunit;
using FieldMapping = Foundatio.Parsers.ElasticQueries.FieldMapping;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public class DailyIndexMappingTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task GetMappingAsync_WithTimeSeriesIndex_UsesAsyncMetadataRequests(bool monthly, bool typed)
    {
        string latest = monthly ? "events-v1-2026.09" : "events-v1-2026.09.07";
        string previous = monthly ? "events-v1-2026.08" : "events-v1-2026.09.06";
        using var invoker = new MappingRequestInvoker(previous, latest);
        using var configuration = new MappingConfiguration(invoker);
        using var index = CreateIndex(configuration, monthly, typed);

        var mapping = await index.MappingResolver.GetMappingAsync("dynamic", cancellationToken: TestContext.Current.CancellationToken);

        Assert.IsType<KeywordProperty>(mapping?.Property);
        Assert.Equal(0, invoker.SyncRequests);
        Assert.Equal(2, invoker.Requests.Count);
        Assert.Contains("features=aliases", invoker.Requests[0].Query);
        Assert.Equal($"/{latest}/_mapping", invoker.Requests[1].AbsolutePath);
    }

    [Fact]
    public async Task GetMappingAsync_WhenDiscoveryIsBlocked_ReturnsWithoutBlockingCaller()
    {
        using var invoker = new MappingRequestInvoker("events-v1-2026.09.07") { BlockDiscovery = true };
        using var configuration = new MappingConfiguration(invoker);
        using var index = new DailyIndex(configuration, "events");

        var lookup = index.MappingResolver.GetMappingAsync("dynamic", cancellationToken: TestContext.Current.CancellationToken).AsTask();
        await invoker.DiscoveryStarted.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        try
        {
            Assert.False(lookup.IsCompleted);
            Assert.Equal(0, invoker.SyncRequests);
        }
        finally
        {
            invoker.ReleaseDiscovery.TrySetResult();
        }

        Assert.True((await lookup)?.Found);
    }

    [Fact]
    public async Task GetMappingAsync_WithConcurrentLookups_SharesMetadataRequests()
    {
        using var invoker = new MappingRequestInvoker("events-v1-2026.09.07") { BlockDiscovery = true };
        using var configuration = new MappingConfiguration(invoker);
        using var index = new DailyIndex(configuration, "events");
        var resolver = index.MappingResolver;
        var lookups = new Task<FieldMapping?>[20];
        for (int i = 0; i < lookups.Length; i++)
            lookups[i] = resolver.GetMappingAsync("dynamic", cancellationToken: TestContext.Current.CancellationToken).AsTask();

        await invoker.DiscoveryStarted.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        try
        {
            Assert.Single(invoker.Requests);
            Assert.All(lookups, lookup => Assert.False(lookup.IsCompleted));
        }
        finally
        {
            invoker.ReleaseDiscovery.TrySetResult();
        }

        var mappings = await Task.WhenAll(lookups);
        Assert.All(mappings, mapping => Assert.True(mapping?.Found));
        Assert.Equal(2, invoker.Requests.Count);
    }

    [Fact]
    public void GetMapping_WithSynchronousCaller_StillResolvesMapping()
    {
        using var invoker = new MappingRequestInvoker("events-v1-2026.09.07");
        using var configuration = new MappingConfiguration(invoker);
        using var index = new DailyIndex(configuration, "events");

        var mapping = index.MappingResolver.GetMapping("dynamic");

        Assert.IsType<KeywordProperty>(mapping?.Property);
        Assert.Equal(2, invoker.Requests.Count);
    }

    [Fact]
    public async Task GetMappingAsync_WithNoPartitions_DoesNotRequestMapping()
    {
        using var invoker = new MappingRequestInvoker();
        using var configuration = new MappingConfiguration(invoker);
        using var index = new DailyIndex(configuration, "events");

        var mapping = await index.MappingResolver.GetMappingAsync("dynamic", cancellationToken: TestContext.Current.CancellationToken);

        Assert.False(mapping?.Found);
        Assert.Single(invoker.Requests);
    }

    [Theory]
    [InlineData(404)]
    [InlineData(503)]
    public async Task GetMappingAsync_WhenDiscoveryFails_DoesNotRequestMapping(int statusCode)
    {
        using var invoker = new MappingRequestInvoker("events-v1-2026.09.07") { DiscoveryStatusCode = statusCode };
        using var configuration = new MappingConfiguration(invoker);
        using var index = new DailyIndex(configuration, "events");

        var mapping = await index.MappingResolver.GetMappingAsync("dynamic", cancellationToken: TestContext.Current.CancellationToken);

        Assert.False(mapping?.Found);
        Assert.Single(invoker.Requests);
    }

    [Theory]
    [InlineData(404)]
    [InlineData(503)]
    public async Task GetMappingAsync_WhenMappingFails_ReturnsUnmapped(int statusCode)
    {
        using var invoker = new MappingRequestInvoker("events-v1-2026.09.07") { MappingStatusCode = statusCode };
        using var configuration = new MappingConfiguration(invoker);
        using var index = new DailyIndex(configuration, "events");

        var mapping = await index.MappingResolver.GetMappingAsync("dynamic", cancellationToken: TestContext.Current.CancellationToken);

        Assert.False(mapping?.Found);
        Assert.Equal(2, invoker.Requests.Count);
    }

    [Fact]
    public async Task RefreshMapping_AfterRollover_LoadsNewestPartition()
    {
        using var invoker = new MappingRequestInvoker("events-v1-2026.09.06");
        using var configuration = new MappingConfiguration(invoker);
        using var index = new DailyIndex(configuration, "events");
        Assert.True((await index.MappingResolver.GetMappingAsync("dynamic", cancellationToken: TestContext.Current.CancellationToken))?.Found);
        invoker.IndexNames = ["events-v1-2026.09.07", "events-v1-2026.09.06"];

        index.MappingResolver.RefreshMapping();
        Assert.True((await index.MappingResolver.GetMappingAsync("dynamic", cancellationToken: TestContext.Current.CancellationToken))?.Found);

        Assert.Equal("/events-v1-2026.09.07/_mapping", invoker.Requests[^1].AbsolutePath);
    }

    [Fact]
    public async Task Dispose_WithMappingLoadInFlight_CancelsTransportRequest()
    {
        using var invoker = new MappingRequestInvoker("events-v1-2026.09.07") { BlockDiscovery = true };
        using var configuration = new MappingConfiguration(invoker);
        using var index = new DailyIndex(configuration, "events");
        var lookup = index.MappingResolver.GetMappingAsync("dynamic", cancellationToken: TestContext.Current.CancellationToken).AsTask();
        await invoker.DiscoveryStarted.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        index.Dispose();
        try
        {
            await invoker.DiscoveryCancelled.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        }
        finally
        {
            invoker.ReleaseDiscovery.TrySetResult();
        }

        await lookup.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.Single(invoker.Requests);
    }

    private static DailyIndex CreateIndex(ElasticConfiguration configuration, bool monthly, bool typed) => (monthly, typed) switch
    {
        (true, true) => new MonthlyIndex<MappingDocument>(configuration, "events"),
        (true, false) => new MonthlyIndex(configuration, "events"),
        (false, true) => new DailyIndex<MappingDocument>(configuration, "events"),
        _ => new DailyIndex(configuration, "events")
    };

    private sealed class MappingDocument;

    private sealed class MappingConfiguration(MappingRequestInvoker invoker) : ElasticConfiguration
    {
        protected override ElasticsearchClient CreateElasticClient() => new(new ElasticsearchClientSettings(
            new SingleNodePool(new Uri("http://localhost:9200")), invoker).DisablePing().MaximumRetries(0));
    }

    private sealed class MappingRequestInvoker(params string[] indexNames)
        : InMemoryRequestInvoker([], 200, headers: new Dictionary<string, IEnumerable<string>> { { "x-elastic-product", ["Elasticsearch"] } }), IRequestInvoker
    {
        public string[] IndexNames { get; set; } = indexNames;
        public int DiscoveryStatusCode { get; init; } = 200;
        public int MappingStatusCode { get; init; } = 200;
        public bool BlockDiscovery { get; init; }
        public int SyncRequests { get; private set; }
        public List<Uri> Requests { get; } = [];
        public TaskCompletionSource DiscoveryStarted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource ReleaseDiscovery { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource DiscoveryCancelled { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public new TResponse Request<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData)
            where TResponse : TransportResponse, new()
        {
            SyncRequests++;
            Requests.Add(endpoint.Uri);
            var (body, statusCode) = GetResponse(endpoint.Uri);
            return BuildResponse<TResponse>(endpoint, boundConfiguration, postData, body, statusCode);
        }

        public new async Task<TResponse> RequestAsync<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration,
            PostData? postData, CancellationToken cancellationToken = default)
            where TResponse : TransportResponse, new()
        {
            Requests.Add(endpoint.Uri);
            if (!endpoint.Uri.AbsolutePath.EndsWith("/_mapping", StringComparison.Ordinal))
            {
                DiscoveryStarted.TrySetResult();
                if (BlockDiscovery)
                {
                    try
                    {
                        await ReleaseDiscovery.Task.WaitAsync(cancellationToken);
                    }
                    catch (OperationCanceledException)
                    {
                        DiscoveryCancelled.TrySetResult();
                        throw;
                    }
                }
            }

            var (body, statusCode) = GetResponse(endpoint.Uri);
            return await BuildResponseAsync<TResponse>(endpoint, boundConfiguration, postData, cancellationToken, body, statusCode);
        }

        private (byte[] Body, int StatusCode) GetResponse(Uri uri)
        {
            bool mapping = uri.AbsolutePath.EndsWith("/_mapping", StringComparison.Ordinal);
            int statusCode = mapping ? MappingStatusCode : DiscoveryStatusCode;
            if (statusCode != 200)
                return (Encoding.UTF8.GetBytes($$"""{"error":{"type":"test_failure","reason":"test failure"},"status":{{statusCode}}}"""), statusCode);

            var response = new Dictionary<string, object>();
            if (mapping)
            {
                string name = uri.AbsolutePath.Split('/')[1];
                response[name] = new { mappings = new { properties = new { dynamic = new { type = "keyword" } } } };
            }
            else
            {
                foreach (string name in IndexNames)
                    response[name] = new { aliases = new { } };
            }

            return (System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(response), statusCode);
        }
    }
}
