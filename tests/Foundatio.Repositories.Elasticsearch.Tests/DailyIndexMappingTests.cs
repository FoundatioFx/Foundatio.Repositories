using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Mapping;
using Elastic.Transport;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Options;
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

    [Theory]
    [InlineData("sort")]
    [InlineData("condition")]
    [InlineData("includes")]
    [InlineData("date-range")]
    [InlineData("default-sort")]
    [InlineData("search-after")]
    public async Task BuildAsync_WithTimeSeriesMapping_ReturnsWithoutBlockingCaller(string operation)
    {
        using var invoker = new MappingRequestInvoker("events-v1-2026.09.07") { BlockDiscovery = true };
        using var configuration = new MappingConfiguration(invoker);
        using var index = new DailyIndex(configuration, "events");
        var query = new RepositoryQuery<MappingDocument>();
        var options = new CommandOptions<MappingDocument>().ElasticIndex(index);
        IElasticQueryBuilder builder;
        switch (operation)
        {
            case "sort":
                query.Sort("dynamic");
                builder = new SortQueryBuilder();
                break;
            case "condition":
                query.FieldEquals("dynamic", "value");
                builder = new FieldConditionsQueryBuilder();
                break;
            case "includes":
                query.Include("dynamic");
                builder = new FieldIncludesQueryBuilder();
                break;
            case "date-range":
                query.DateRange(DateTime.UtcNow.AddDays(-1), DateTime.UtcNow, "dynamic");
                builder = new DateRangeQueryBuilder();
                break;
            case "default-sort":
                builder = new DefaultSortQueryBuilder();
                break;
            default:
                options.SearchAfterPaging();
                builder = new SearchAfterQueryBuilder();
                break;
        }

        var context = new QueryBuilderContext<MappingDocument>(query, options);
        var returned = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var invocation = Task.Run(async () =>
        {
            var build = builder.BuildAsync(context);
            returned.TrySetResult();
            await build;
        }, TestContext.Current.CancellationToken);

        try
        {
            await invoker.DiscoveryStarted.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
            await returned.Task.WaitAsync(TimeSpan.FromSeconds(1), TestContext.Current.CancellationToken);
            Assert.False(invocation.IsCompleted);
            Assert.Equal(0, invoker.SyncRequests);
        }
        finally
        {
            invoker.ReleaseDiscovery.TrySetResult();
            await invocation.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        }

        Assert.Equal(2, invoker.Requests.Count);
    }

    [Fact]
    public async Task GetResolvedFieldsAsync_WithBoostsAndSortVariants_PreservesRequestSettings()
    {
        using var settings = new ElasticsearchClientSettings(new Uri("http://localhost:9200"));
        using var resolver = ElasticMappingResolver.CreateWithAsyncLoader(_ => Task.FromResult<TypeMapping?>(new TypeMapping
        {
            Properties = new Properties
            {
                { "name", new TextProperty { Fields = new Properties { { "keyword", new KeywordProperty() } } } }
            }
        }), new Inferrer(settings));
        var score = new SortOptions { Score = new ScoreSort { Order = SortOrder.Desc } };
        SortOptions field = new FieldSort { Field = "name", Order = SortOrder.Desc, Mode = SortMode.Max };

        var fields = await resolver.GetResolvedFieldsAsync(new List<Field> { new("name", 2) }, TestContext.Current.CancellationToken);
        var sorts = await resolver.GetResolvedFieldsAsync(new List<SortOptions> { field, score, null! }, TestContext.Current.CancellationToken);

        var resolvedField = Assert.Single(fields);
        Assert.Equal("name", resolvedField.Name);
        Assert.Equal(2, resolvedField.Boost);
        Assert.Equal(2, sorts.Count);
        Assert.Contains(score, sorts);
        var resolvedSort = Assert.Single(sorts, sort => sort.Field is not null).Field!;
        Assert.Equal("name.keyword", resolvedSort.Field.Name);
        Assert.Equal(SortOrder.Desc, resolvedSort.Order);
        Assert.Equal(SortMode.Max, resolvedSort.Mode);
        Assert.Equal("name", field.Field!.Field.Name);
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

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Dispose_WithResolverNotYetPublished_PreventsLaterMappingLoads(bool creationStarted)
    {
        using var invoker = new MappingRequestInvoker("events-v1-2026.09.07");
        using var configuration = new MappingConfiguration(invoker);
        using var started = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        using var index = new BlockingMappingIndex(configuration, started, release);
        Task<ElasticMappingResolver>? creation = null;
        try
        {
            if (creationStarted)
            {
                creation = Task.Factory.StartNew(() => index.MappingResolver, TestContext.Current.CancellationToken,
                    TaskCreationOptions.LongRunning, TaskScheduler.Default);
                Assert.True(started.Wait(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));
            }

            index.Dispose();
            Assert.Equal(creationStarted, started.IsSet);
        }
        finally
        {
            release.Set();
            if (creation is not null)
                await creation.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        }

        var mapping = await index.MappingResolver.GetMappingAsync("dynamic", cancellationToken: TestContext.Current.CancellationToken);
        Assert.False(mapping?.Found);
        Assert.Empty(invoker.Requests);
    }

    private static DailyIndex CreateIndex(ElasticConfiguration configuration, bool monthly, bool typed) => (monthly, typed) switch
    {
        (true, true) => new MonthlyIndex<MappingDocument>(configuration, "events"),
        (true, false) => new MonthlyIndex(configuration, "events"),
        (false, true) => new DailyIndex<MappingDocument>(configuration, "events"),
        _ => new DailyIndex(configuration, "events")
    };

    private sealed class BlockingMappingIndex(IElasticConfiguration configuration, ManualResetEventSlim started,
        ManualResetEventSlim release) : DailyIndex(configuration, "events")
    {
        protected override ElasticMappingResolver CreateMappingResolver()
        {
            started.Set();
            Assert.True(release.Wait(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));
            return base.CreateMappingResolver();
        }
    }

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
