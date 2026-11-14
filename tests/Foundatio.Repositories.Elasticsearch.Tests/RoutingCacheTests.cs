using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Transport;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Queries;
using Foundatio.Serializer;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class RoutingCacheTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PatchAsync_WithRouting_InvalidatesAndNotifiesTheRawDocumentId(bool script)
    {
        var stored = new StoredDocument("same", "second", "before", 2, 22);
        var collision = new StoredDocument("second-same", null, "unrelated value", 3, 33);
        using var configuration = new TestConfiguration([stored, collision]);
        using var repository = new TestRepository(configuration);
        await repository.SeedAsync(stored.Id, [stored.ToHit()]);
        await repository.SeedAsync(collision.Id, [collision.ToHit()]);
        IPatchOperation patch = script ? new ScriptPatch("ctx._source.value = 'patched';") : new PartialPatch(new { value = "patched" });

        bool patched = await repository.PatchAsync(new Id(stored.Id, stored.Routing), patch, o => o.Retry(0));

        Assert.True(patched);
        Assert.False(await repository.CacheContainsAsync(stored.Id));
        Assert.True(await repository.CacheContainsAsync(collision.Id));
        Assert.Equal((ChangeType.Saved, stored.Id), Assert.Single(repository.PublishedChanges));
        Assert.Equal(new Id(stored.Id, stored.Routing), Assert.Single(configuration.Invoker.UpdatedLocations));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task GetByIdAsync_WithRoutedCachedDocuments_ReturnsTheRequestedLocationWithoutReadingElasticsearch(bool customCacheKey)
    {
        var first = new StoredDocument("same", "first", "first value", 1, 11);
        var second = new StoredDocument("same", "second", "second value", 2, 22);
        using var configuration = new TestConfiguration([first, second]);
        using var repository = new TestRepository(configuration);
        string cacheKey = customCacheKey ? "selected-documents" : first.Id;
        await repository.SeedAsync(cacheKey, [first.ToHit(), second.ToHit()]);
        var options = new CommandOptions().Cache();
        if (customCacheKey)
            options.CacheKey(cacheKey);

        var result = await repository.GetByIdAsync(new Id(second.Id, second.Routing), options);

        Assert.NotNull(result);
        AssertDocument(result, second);
        Assert.Empty(configuration.Invoker.ReadLocations);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task GetByIdAsync_WhenOnlyAnotherRoutingIsCached_ReadsTheRequestedLocation(bool customCacheKey)
    {
        var cached = new StoredDocument("same", "first", "unrequested value", 1, 11);
        var requested = new StoredDocument("same", "second", "requested value", 2, 22);
        using var configuration = new TestConfiguration([cached, requested]);
        using var repository = new TestRepository(configuration);
        string cacheKey = customCacheKey ? "selected-documents" : cached.Id;
        await repository.SeedAsync(cacheKey, [cached.ToHit()]);
        var options = new CommandOptions().Cache();
        if (customCacheKey)
            options.CacheKey(cacheKey);

        var result = await repository.GetByIdAsync(new Id(requested.Id, requested.Routing), options);

        Assert.NotNull(result);
        AssertDocument(result, requested);
        Assert.Equal(new Id(requested.Id, requested.Routing), Assert.Single(configuration.Invoker.ReadLocations));
    }

    [Fact]
    public async Task GetByIdAsync_WhenRoutingAndIdMatchAnotherRawCacheKey_DoesNotReturnTheOtherDocument()
    {
        var requested = new StoredDocument("same", "second", "requested value", 2, 22);
        var collision = new StoredDocument("second-same", null, "unrelated value", 3, 33);
        using var configuration = new TestConfiguration([requested, collision]);
        using var repository = new TestRepository(configuration);
        await repository.SeedAsync(requested.Id, [requested.ToHit()]);
        await repository.SeedAsync(collision.Id, [collision.ToHit()]);

        var result = await repository.GetByIdAsync(new Id(requested.Id, requested.Routing), new CommandOptions().Cache());

        Assert.NotNull(result);
        AssertDocument(result, requested);
        Assert.Empty(configuration.Invoker.ReadLocations);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task GetByIdsAsync_WithSameIdAndDifferentRouting_ReturnsEachLocationOnce(bool allCached, bool customCacheKey)
    {
        var first = new StoredDocument("same", "first", "first value", 1, 11);
        var second = new StoredDocument("same", "second", "second value", 2, 22);
        using var configuration = new TestConfiguration([first, second]);
        using var repository = new TestRepository(configuration);
        string cacheKey = customCacheKey ? "selected-documents" : first.Id;
        await repository.SeedAsync(cacheKey, allCached ? [first.ToHit(), second.ToHit()] : [first.ToHit()]);
        var options = new CommandOptions().Cache();
        if (customCacheKey)
            options.CacheKey(cacheKey);

        var results = await repository.GetByIdsAsync(new Ids(new Id(first.Id, first.Routing), new Id(second.Id, second.Routing)), options);

        Assert.Equal(2, results.Count);
        AssertDocument(Assert.Single(results, document => document.Route == first.Routing), first);
        AssertDocument(Assert.Single(results, document => document.Route == second.Routing), second);
        if (allCached)
            Assert.Empty(configuration.Invoker.ReadLocations);
        else
            Assert.Equal(new Id(second.Id, second.Routing), Assert.Single(configuration.Invoker.ReadLocations));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task GetByIdsAsync_WhenOnlyAnotherRoutingIsCached_DoesNotReturnTheCachedDocument(bool customCacheKey)
    {
        var cached = new StoredDocument("same", "first", "unrequested value", 1, 11);
        var requested = new StoredDocument("same", "second", "requested value", 2, 22);
        using var configuration = new TestConfiguration([cached, requested]);
        using var repository = new TestRepository(configuration);
        string cacheKey = customCacheKey ? "selected-documents" : cached.Id;
        await repository.SeedAsync(cacheKey, [cached.ToHit()]);
        var options = new CommandOptions().Cache();
        if (customCacheKey)
            options.CacheKey(cacheKey);

        var results = await repository.GetByIdsAsync(new Ids(new Id(requested.Id, requested.Routing)), options);

        AssertDocument(Assert.Single(results), requested);
        Assert.Equal(new Id(requested.Id, requested.Routing), Assert.Single(configuration.Invoker.ReadLocations));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task GetByIdsAsync_WithUnroutedCachedDocument_RestoresMetadataWithoutReadingElasticsearch(bool customCacheKey)
    {
        var stored = new StoredDocument("unrouted", null, "cached value", 3, 33);
        using var configuration = new TestConfiguration([stored]);
        using var repository = new TestRepository(configuration, hasParent: false);
        string cacheKey = customCacheKey ? "selected-documents" : stored.Id;
        await repository.SeedAsync(cacheKey, [stored.ToHit()]);
        var options = new CommandOptions().Cache();
        if (customCacheKey)
            options.CacheKey(cacheKey);

        var results = await repository.GetByIdsAsync(new Ids(stored.Id), options);

        AssertDocument(Assert.Single(results), stored);
        Assert.Empty(configuration.Invoker.ReadLocations);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task GetByIdsAsync_WhenRequestedRoutingIsMissing_DoesNotAddOtherRoutesFromFallbackSearch(bool includeFoundRoute)
    {
        var stored = new StoredDocument("same", "first", "first value", 1, 11);
        using var configuration = new TestConfiguration([stored]);
        using var repository = new TestRepository(configuration);
        var missing = new Id(stored.Id, "missing");
        var ids = includeFoundRoute ? new Ids(new Id(stored.Id, stored.Routing), missing) : new Ids(missing);

        var results = await repository.GetByIdsAsync(ids, new CommandOptions().Cache());

        if (includeFoundRoute)
            AssertDocument(Assert.Single(results), stored);
        else
            Assert.Empty(results);
    }

    [Fact]
    public async Task GetByIdsAsync_WithSameIdInDifferentSearchIndexes_ReturnsBothDocuments()
    {
        var first = new StoredDocument("same", null, "first index", 1, 11, "routing-cache-first");
        var second = new StoredDocument("same", null, "second index", 2, 22, "routing-cache-second");
        using var configuration = new TestConfiguration([first, second]);
        using var repository = new TestRepository(configuration);

        var results = await repository.GetByIdsAsync(new Ids(first.Id), new CommandOptions().Cache(false));

        Assert.Equal(2, results.Count);
        Assert.Equal(new[] { first.Value, second.Value }, results.Select(document => document.Value));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task GetByIdsAsync_WithSameIdInDifferentCachedIndexes_ReturnsBothDocuments(bool customCacheKey)
    {
        var first = new StoredDocument("same", null, "first index", 1, 11, "routing-cache-first");
        var second = new StoredDocument("same", null, "second index", 2, 22, "routing-cache-second");
        using var configuration = new TestConfiguration([first, second]);
        using var repository = new TestRepository(configuration);
        string cacheKey = customCacheKey ? "selected-documents" : first.Id;
        var duplicate = first with { Value = "duplicate" };
        var hits = new List<FindHit<RoutedDocument>> { first.ToHit(), duplicate.ToHit(), second.ToHit() };
        var cachedHits = Assert.IsType<List<FindHit<RoutedDocument>>>(configuration.Serializer.Deserialize<List<FindHit<RoutedDocument>>>(configuration.Serializer.SerializeToBytes(hits)));
        await repository.SeedAsync(cacheKey, cachedHits);
        var options = new CommandOptions().Cache();
        if (customCacheKey)
            options.CacheKey(cacheKey);

        var results = await repository.GetByIdsAsync(new Ids(first.Id), options);

        Assert.Equal(2, results.Count);
        AssertDocument(Assert.Single(results, document => document.Value == first.Value), first);
        AssertDocument(Assert.Single(results, document => document.Value == second.Value), second);
        Assert.Empty(configuration.Invoker.ReadLocations);
    }

    [Fact]
    public async Task GetByIdsAsync_WithRoutedAndUnroutedInputs_SearchesForOtherMatchingRoutes()
    {
        var first = new StoredDocument("same", "first", "first value", 1, 11);
        var second = new StoredDocument("same", "second", "second value", 2, 22);
        using var configuration = new TestConfiguration([first, second]);
        using var repository = new TestRepository(configuration);

        var results = await repository.GetByIdsAsync(new Ids(new Id(first.Id, first.Routing), new Id(first.Id)), new CommandOptions().Cache(false));

        Assert.Equal(2, results.Count);
        Assert.Equal(new[] { first.Routing, second.Routing }, results.Select(document => document.Route).Order());
        Assert.Equal(new Id(first.Id, first.Routing), Assert.Single(configuration.Invoker.ReadLocations));
    }

    private static void AssertDocument(RoutedDocument document, StoredDocument expected)
    {
        Assert.Equal(expected.Id, document.Id);
        Assert.Equal(expected.Routing, document.Route);
        Assert.Equal(expected.Value, document.Value);
        Assert.Equal(expected.Version, ((IVersioned)document).Version);
    }

    public sealed class RoutedDocument : IIdentity, IVersioned
    {
        public string Id { get; set; } = null!;
        public string? Route { get; set; }
        public string Value { get; set; } = null!;
        string? IVersioned.Version { get; set; }
    }

    private sealed record StoredDocument(string Id, string? Routing, string Value, long PrimaryTerm, long SequenceNumber, string Index = "routing-cache-tests")
    {
        public string Version => $"{PrimaryTerm}:{SequenceNumber}";
        public FindHit<RoutedDocument> ToHit() =>
            new(Id, new RoutedDocument { Id = Id, Route = Routing, Value = Value }, 0, Version, Routing,
                new Dictionary<string, object?> { [ElasticDataKeys.Index] = Index });
    }

    private sealed class TestRepository : ElasticRepositoryBase<RoutedDocument>
    {
        private readonly IReadOnlyList<StoredDocument> _documents;

        public TestRepository(TestConfiguration configuration, bool hasParent = true)
            : base(new TestIndex(configuration))
        {
            _documents = configuration.Documents;
            HasParent = hasParent;
            GetParentIdFunc = document => document.Route!;
        }

        public Task SeedAsync(string key, ICollection<FindHit<RoutedDocument>> hits) => Cache.SetAsync(key, hits);
        public Task<bool> CacheContainsAsync(string key) => Cache.ExistsAsync(key);
        public List<(ChangeType ChangeType, string? Id)> PublishedChanges { get; } = [];

        protected override Task PublishChangeTypeMessageAsync(ChangeType changeType, string? id, IDictionary<string, object?>? data = null, TimeSpan? delay = null)
        {
            PublishedChanges.Add((changeType, id));
            return Task.CompletedTask;
        }

        public override Task<FindResults<TResult>> FindAsAsync<TResult>(IRepositoryQuery query, ICommandOptions? options = null)
        {
            var ids = query.GetIds();
            var hits = _documents.Where(document => ids.Contains(document.Id))
                .Select(document =>
                {
                    var hit = document.ToHit();
                    return new FindHit<TResult>(hit.Id, (TResult)(object)hit.Document!, 0, hit.Version, hit.Routing, hit.Data);
                });
            return Task.FromResult(new FindResults<TResult>(hits));
        }
    }

    private sealed class TestIndex(TestConfiguration configuration) : Index<RoutedDocument>(configuration, "routing-cache-tests")
    {
        public override Task EnsureIndexAsync(object? target) => Task.CompletedTask;
    }

    private sealed class TestConfiguration(IReadOnlyList<StoredDocument> documents) : ElasticConfiguration
    {
        public IReadOnlyList<StoredDocument> Documents => documents;
        public RoutingReadInvoker Invoker { get; } = new(documents);
        protected override IRequestInvoker CreateRequestInvoker() => Invoker;
    }

    private sealed class RoutingReadInvoker(IReadOnlyList<StoredDocument> documents) : IRequestInvoker
    {
        private readonly InMemoryRequestInvoker _owner = new();
        private static readonly Dictionary<string, IEnumerable<string>> _headers = new(StringComparer.OrdinalIgnoreCase) { ["X-Elastic-Product"] = ["Elasticsearch"] };
        public ResponseFactory ResponseFactory => _owner.ResponseFactory;
        public List<Id> ReadLocations { get; } = [];
        public List<Id> UpdatedLocations { get; } = [];

        public TResponse Request<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData)
            where TResponse : TransportResponse, new()
        {
            object response;
            if (endpoint.PathAndQuery.Contains("/_update/", StringComparison.Ordinal))
            {
                var location = GetLocation(endpoint);
                UpdatedLocations.Add(location);
                var stored = Assert.Single(documents, document => document.Id == location.Value && document.Routing == location.Routing);
                response = new
                {
                    _index = "routing-cache-tests",
                    _id = stored.Id,
                    _version = 2,
                    result = "updated",
                    _seq_no = stored.SequenceNumber + 1,
                    _primary_term = stored.PrimaryTerm,
                    _shards = new { total = 1, successful = 1, failed = 0 }
                };
            }
            else if (endpoint.PathAndQuery.Contains("_mget", StringComparison.Ordinal))
            {
                Assert.NotNull(postData);
                using var requestStream = new MemoryStream();
                postData.Write(requestStream, boundConfiguration.ConnectionSettings, false);
                using var request = JsonDocument.Parse(requestStream.ToArray());
                var results = new List<object>();
                foreach (var item in request.RootElement.GetProperty("docs").EnumerateArray())
                {
                    string id = item.GetProperty("_id").GetString()!;
                    string? routing = item.TryGetProperty("routing", out var route) ? route.GetString() : null;
                    results.Add(ReadDocument(id, routing));
                }

                response = new { docs = results };
            }
            else
            {
                Assert.Contains("/_doc/", endpoint.PathAndQuery, StringComparison.Ordinal);
                var location = GetLocation(endpoint);
                response = ReadDocument(location.Value, location.Routing);
            }

            byte[] bytes = JsonSerializer.SerializeToUtf8Bytes(response);
            using var stream = new MemoryStream(bytes);
            return ResponseFactory.Create<TResponse>(endpoint, boundConfiguration, postData, null, 200, _headers, stream, "application/json", bytes.Length, null, null);
        }

        private static Id GetLocation(Endpoint endpoint)
        {
            var uri = new Uri($"http://localhost{endpoint.PathAndQuery}");
            string id = Uri.UnescapeDataString(uri.Segments[^1]);
            string? routing = uri.Query.TrimStart('?').Split('&')
                .FirstOrDefault(value => value.StartsWith("routing=", StringComparison.Ordinal))?["routing=".Length..];
            return new Id(id, routing is null ? null : Uri.UnescapeDataString(routing));
        }

        private object ReadDocument(string id, string? routing)
        {
            ReadLocations.Add(new Id(id, routing));
            var stored = documents.SingleOrDefault(document => document.Id == id && document.Routing == routing);
            return new
            {
                _index = "routing-cache-tests",
                _id = id,
                _routing = routing,
                found = stored is not null,
                _seq_no = stored?.SequenceNumber,
                _primary_term = stored?.PrimaryTerm,
                _source = stored is null ? null : new { id = stored.Id, route = stored.Routing, value = stored.Value }
            };
        }

        public Task<TResponse> RequestAsync<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData, CancellationToken cancellationToken)
            where TResponse : TransportResponse, new()
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(Request<TResponse>(endpoint, boundConfiguration, postData));
        }

        public void Dispose() => ((IDisposable)_owner).Dispose();
    }
}
