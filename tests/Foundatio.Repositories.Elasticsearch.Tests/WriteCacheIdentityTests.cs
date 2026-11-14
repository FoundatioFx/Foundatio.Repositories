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
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;
using Xunit;
using HttpMethod = Elastic.Transport.HttpMethod;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class WriteCacheIdentityTests
{
    [Theory]
    [InlineData("Add", false)]
    [InlineData("Add", true)]
    [InlineData("Save", false)]
    [InlineData("Save", true)]
    public async Task CachedWrite_WithSameIdAndRoutingInDifferentIndexes_RetainsEachPhysicalLocation(string operation, bool customCacheKey)
    {
        using var configuration = new TestConfiguration();
        using var repository = new TestRepository(configuration);
        TestDocument[] documents =
        [
            new() { Id = "same", Index = "first", Routing = "route", Value = "first document", Version = operation is "Save" ? "1:1" : null },
            new() { Id = "same", Index = "second", Routing = "route", Value = "second document", Version = operation is "Save" ? "1:1" : null }
        ];
        var options = new CommandOptions().Cache().Retry(0);
        if (customCacheKey)
            options.CacheKey("selected-documents");

        if (operation is "Add")
            await repository.AddAsync(documents, options);
        else
            await repository.SaveAsync(documents, options);

        var results = await repository.GetByIdsAsync(new Ids(new Id("same", "route")), options);

        Assert.Equal(2, results.Count);
        Assert.Equal(documents.Select(document => document.Value), results.Select(document => document.Value));
        Assert.Equal(new[] { "3:10", "3:20" }, results.Select(document => document.Version));
        Assert.Equal(new[] { ("first", "same", "route"), ("second", "same", "route") }, configuration.Invoker.Writes);
        Assert.Equal(1, configuration.Invoker.Requests);
        var cached = await repository.GetCachedHitsAsync(customCacheKey ? "selected-documents" : "same");
        Assert.Equal(new[] { "first-v1", "second-v1" }, cached.Select(hit => hit.GetIndex()));
    }

    [Theory]
    [InlineData("Add", false)]
    [InlineData("Add", true)]
    [InlineData("Save", false)]
    [InlineData("Save", true)]
    public async Task CachedSingleWrite_ThroughAnAlias_RetainsTheResponsePhysicalIndex(string operation, bool customCacheKey)
    {
        var document = new TestDocument { Id = "one", Index = "first", Routing = "route", Value = "written value", Version = operation is "Save" ? "1:1" : null };
        using var configuration = new TestConfiguration(operation is "Save" ? [document] : []);
        using var repository = new TestRepository(configuration);
        var options = new CommandOptions().Cache().Retry(0);
        if (customCacheKey)
            options.CacheKey("selected-document");

        if (operation is "Add")
            await repository.AddAsync(document, options);
        else
            await repository.SaveAsync(document, options);

        var result = await repository.GetByIdAsync(new Id(document.Id, document.Routing), options);

        Assert.NotNull(result);
        Assert.Equal(document.Value, result.Value);
        Assert.Equal("3:10", result.Version);
        Assert.Equal(1, configuration.Invoker.IndexRequests);
        Assert.Equal(1, configuration.Invoker.Requests);
        Assert.Empty(configuration.Invoker.Reads);
        var cached = Assert.Single(await repository.GetCachedHitsAsync(customCacheKey ? "selected-document" : document.Id));
        Assert.Equal("first-v1", cached.GetIndex());
        Assert.Equal("route", cached.Routing);
        Assert.Equal("3:10", cached.Version);
    }

    [Theory]
    [InlineData("Add", false)]
    [InlineData("Add", true)]
    [InlineData("Save", false)]
    [InlineData("Save", true)]
    public async Task CachedWrite_WhenPostWriteHandlerSavesTheSameModel_PreservesTheNewerVersion(string operation, bool customCacheKey)
    {
        using var configuration = new TestConfiguration();
        using var repository = new TestRepository(configuration);
        var document = new TestDocument { Id = "one", Index = "first", Routing = "route", Value = "outer write" };
        var options = new CommandOptions().Cache().Retry(0);
        if (customCacheKey)
            options.CacheKey("selected-document");
        if (operation is "Save")
        {
            await repository.AddAsync(document, new CommandOptions().Retry(0));
            if (customCacheKey)
            {
                var original = await repository.GetByIdAsync(new Id(document.Id, document.Routing), options);
                Assert.NotNull(original);
                Assert.Equal("outer write", original.Value);
                Assert.Equal("3:10", original.Version);
                var cached = Assert.Single(await repository.GetCachedHitsAsync("selected-document"));
                Assert.Equal("3:10", cached.Version);
            }
        }

        bool savedFromHandler = false;
        async Task SaveFromHandlerAsync(TestDocument current)
        {
            if (savedFromHandler)
                return;

            savedFromHandler = true;
            Assert.Same(document, current);
            current.Value = "nested save";
            await repository.SaveAsync(current, new CommandOptions().Cache(false).Retry(0));
        }
        if (operation is "Add")
            repository.DocumentsAdded.AddHandler((_, args) => SaveFromHandlerAsync(Assert.Single(args.Documents)));
        else
            repository.DocumentsSaved.AddHandler((_, args) => SaveFromHandlerAsync(Assert.Single(args.Documents).Value));

        if (operation is "Add")
            await repository.AddAsync(document, options);
        else
            await repository.SaveAsync(document, options);

        Assert.True(savedFromHandler);
        string latestVersion = operation is "Add" ? "3:20" : "3:30";
        Assert.Equal(latestVersion, document.Version);
        var current = await repository.GetByIdAsync(new Id(document.Id, document.Routing), options);
        Assert.NotNull(current);
        Assert.Equal("nested save", current.Value);
        Assert.Equal(latestVersion, current.Version);

        current.Value = "after nested save";
        await repository.SaveAsync(current, new CommandOptions().Retry(0));

        Assert.Equal(operation is "Add" ? "3:30" : "3:40", current.Version);
        var persisted = await repository.GetByIdAsync(new Id(document.Id, document.Routing), new CommandOptions().Cache(false));
        Assert.NotNull(persisted);
        Assert.Equal(current.Value, persisted.Value);
        Assert.Equal(current.Version, persisted.Version);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task GetByIdsAsync_WithCachedRouteAndUnroutedInput_FindsOtherMatchingRoutes(bool customCacheKey, bool readOnlyCache)
    {
        TestDocument[] documents =
        [
            new() { Id = "same", Index = "first", Routing = "first", Value = "first route" },
            new() { Id = "same", Index = "first", Routing = "second", Value = "second route" }
        ];
        using var configuration = new TestConfiguration(documents, hasMultipleIndexes: false);
        using var repository = new TestRepository(configuration);
        var options = new CommandOptions().Cache();
        if (customCacheKey)
            options.CacheKey("selected-documents");
        Assert.NotNull(await repository.GetByIdAsync(new Id("same", "first"), options));
        if (readOnlyCache)
            options.Cache(false).ReadCache();

        var results = await repository.GetByIdsAsync(new Ids(new Id("same", "first"), new Id("same")), options);

        Assert.Equal(documents.Select(document => document.Value), results.Select(document => document.Value));
        Assert.Equal(new Id("same", "first"), Assert.Single(configuration.Invoker.Reads));
        Assert.Equal(1, configuration.Invoker.Searches);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task GetByIdsAsync_WithOnlyExplicitRoutes_UsesCachedDocuments(bool customCacheKey)
    {
        using var configuration = new TestConfiguration(hasMultipleIndexes: false);
        using var repository = new TestRepository(configuration);
        TestDocument[] documents =
        [
            new() { Id = "same", Index = "first", Routing = "first", Value = "first route" },
            new() { Id = "same", Index = "first", Routing = "second", Value = "second route" }
        ];
        var options = new CommandOptions().Cache().Retry(0);
        if (customCacheKey)
            options.CacheKey("selected-documents");
        await repository.AddAsync(documents, options);

        var results = await repository.GetByIdsAsync(new Ids(new Id("same", "first"), new Id("same", "second")), options);

        Assert.Equal(documents.Select(document => document.Value), results.Select(document => document.Value));
        Assert.Equal(1, configuration.Invoker.Requests);
        Assert.Empty(configuration.Invoker.Reads);
        Assert.Equal(0, configuration.Invoker.Searches);
    }

    [Theory]
    [InlineData("Add", false)]
    [InlineData("Add", true)]
    [InlineData("Save", false)]
    [InlineData("Save", true)]
    public async Task CachedWrite_WithIngestPipeline_ReturnsTransformedSource(string operation, bool customCacheKey)
    {
        TestDocument[] documents =
        [
            new() { Id = "one", Index = "first", Routing = "route", Value = "FIRST VALUE", Version = operation is "Save" ? "1:1" : null },
            new() { Id = "two", Index = "first", Routing = "route", Value = "SECOND VALUE", Version = operation is "Save" ? "1:1" : null }
        ];
        var originals = operation is "Save"
            ? documents.Select(document => new TestDocument { Id = document.Id, Index = document.Index, Routing = document.Routing, Value = "original", Version = "1:1" }).ToArray()
            : [];
        using var configuration = new TestConfiguration(originals, hasMultipleIndexes: false);
        using var repository = new TestRepository(configuration, "lowercase-value");
        var options = new CommandOptions().Cache().Retry(0);
        if (customCacheKey)
            options.CacheKey("selected-documents");
        foreach (var original in originals)
            Assert.NotNull(await repository.GetByIdAsync(new Id(original.Id, original.Routing), options));

        if (operation is "Add")
            await repository.AddAsync(documents, options);
        else
            await repository.SaveAsync(documents, options);

        foreach (var document in documents.Reverse())
        {
            var result = await repository.GetByIdAsync(new Id(document.Id, document.Routing), options);
            Assert.NotNull(result);
            Assert.Equal(document.Value.ToLowerInvariant(), result.Value);
            Assert.Equal(document.Version, result.Version);
        }
        Assert.Equal(documents.Length, configuration.Invoker.PipelineWrites);
    }

    public sealed class TestDocument : IIdentity, IVersioned
    {
        public string Id { get; set; } = null!;
        public string Index { get; set; } = null!;
        public string Routing { get; set; } = null!;
        public string Value { get; set; } = null!;
        public string? Version { get; set; }
    }

    private sealed class TestRepository : ElasticRepositoryBase<TestDocument>
    {
        public TestRepository(TestConfiguration configuration, string? pipeline = null) : base(new TestIndex(configuration))
        {
            HasParent = true;
            GetParentIdFunc = document => document.Routing;
            NotificationsEnabled = false;
            DefaultPipeline = pipeline;
        }

        public async Task<ICollection<FindHit<TestDocument>>> GetCachedHitsAsync(string key)
        {
            var value = await Cache.GetAsync<ICollection<FindHit<TestDocument>>>(key);
            Assert.True(value.HasValue);
            return value.Value;
        }
    }

    private sealed class TestIndex : Index<TestDocument>
    {
        public TestIndex(TestConfiguration configuration) : base(configuration, "write-cache-tests")
        {
            HasMultipleIndexes = configuration.HasMultipleIndexes;
        }

        public override string GetIndex(object target) => target is TestDocument document ? document.Index : "first";
        public override Task EnsureIndexAsync(object? target) => Task.CompletedTask;
    }

    private sealed class TestConfiguration(IReadOnlyList<TestDocument>? documents = null, bool hasMultipleIndexes = true) : ElasticConfiguration
    {
        public bool HasMultipleIndexes { get; } = hasMultipleIndexes;
        public CacheResponseInvoker Invoker { get; } = new(documents ?? []);
        protected override IRequestInvoker CreateRequestInvoker() => Invoker;
    }

    private sealed class CacheResponseInvoker(IReadOnlyList<TestDocument> documents) : IRequestInvoker
    {
        private readonly InMemoryRequestInvoker _owner = new();
        private readonly Dictionary<(string Index, string Id, string Routing), TestDocument> _documents = documents.ToDictionary(document => (document.Index, document.Id, document.Routing));
        private static readonly Dictionary<string, IEnumerable<string>> _headers = new(StringComparer.OrdinalIgnoreCase) { ["X-Elastic-Product"] = ["Elasticsearch"] };
        public ResponseFactory ResponseFactory => _owner.ResponseFactory;
        public int Requests { get; private set; }
        public int IndexRequests { get; private set; }
        public int Searches { get; private set; }
        public int PipelineWrites { get; private set; }
        public List<(string Index, string Id, string Routing)> Writes { get; } = [];
        public List<Id> Reads { get; } = [];

        public TResponse Request<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData)
            where TResponse : TransportResponse, new()
        {
            Requests++;
            int statusCode = 200;
            object response;
            if (endpoint.PathAndQuery.Contains("_bulk", StringComparison.Ordinal))
            {
                response = CreateBulkResponse(boundConfiguration, postData);
            }
            else if (endpoint.Method is HttpMethod.PUT && endpoint.PathAndQuery.Contains("/_doc/", StringComparison.Ordinal))
            {
                IndexRequests++;
                var uri = new Uri(new Uri("http://localhost/"), endpoint.PathAndQuery);
                string index = Uri.UnescapeDataString(uri.Segments[1].TrimEnd('/'));
                string id = Uri.UnescapeDataString(uri.Segments[^1]);
                var parameters = uri.Query.TrimStart('?').Split('&', StringSplitOptions.RemoveEmptyEntries)
                    .Select(parameter => parameter.Split('=', 2))
                    .ToDictionary(pair => Uri.UnescapeDataString(pair[0]), pair => Uri.UnescapeDataString(pair[1]));
                string routing = parameters["routing"];
                var key = (index, id, routing);
                _documents.TryGetValue(key, out var stored);
                var storedVersion = ElasticDocumentVersion.Parse(stored?.Version);
                if (parameters.TryGetValue("if_seq_no", out string? sequenceNumber)
                    && (storedVersion.SequenceNumber != Int64.Parse(sequenceNumber)
                        || storedVersion.PrimaryTerm != Int64.Parse(parameters["if_primary_term"])))
                {
                    statusCode = 409;
                    response = new { status = 409, error = new { type = "version_conflict_engine_exception", reason = "The conditional write used a stale version." } };
                }
                else
                {
                    Assert.NotNull(postData);
                    using var requestStream = new MemoryStream();
                    postData.Write(requestStream, boundConfiguration.ConnectionSettings, false);
                    using var source = JsonDocument.Parse(requestStream.ToArray());
                    string value = source.RootElement.GetProperty("value").GetString()!;
                    Writes.Add(key);
                    _documents[key] = new TestDocument { Id = id, Index = index, Routing = routing, Value = value, Version = $"3:{Writes.Count * 10}" };
                    response = new { _index = $"{index}-v1", _id = id, result = parameters.GetValueOrDefault("op_type") is "create" ? "created" : "updated", _seq_no = Writes.Count * 10, _primary_term = 3, _version = Writes.Count, _shards = new { total = 1, successful = 1, failed = 0 } };
                }
            }
            else if (endpoint.PathAndQuery.Contains("_search", StringComparison.Ordinal))
            {
                Searches++;
                var hits = _documents.Values.Select(document => new
                {
                    _index = $"{document.Index}-v1",
                    _id = document.Id,
                    _routing = document.Routing,
                    _seq_no = 10,
                    _primary_term = 3,
                    _source = new { id = document.Id, index = document.Index, routing = document.Routing, value = document.Value }
                });
                response = new { took = 1, timed_out = false, hits = new { total = new { value = _documents.Count, relation = "eq" }, hits } };
            }
            else if (endpoint.PathAndQuery.Contains("_mapping", StringComparison.Ordinal))
            {
                response = new Dictionary<string, object> { ["write-cache-tests"] = new { mappings = new { properties = new { id = new { type = "keyword" } } } } };
            }
            else
            {
                Assert.Contains("/_doc/", endpoint.PathAndQuery, StringComparison.Ordinal);
                var uri = new Uri(new Uri("http://localhost/"), endpoint.PathAndQuery);
                string id = Uri.UnescapeDataString(uri.Segments[^1]);
                string? routing = uri.Query.TrimStart('?').Split('&')
                    .FirstOrDefault(value => value.StartsWith("routing=", StringComparison.Ordinal))?["routing=".Length..];
                routing = routing is null ? null : Uri.UnescapeDataString(routing);
                Reads.Add(new Id(id, routing));
                var document = Assert.Single(_documents.Values, document => document.Id == id && document.Routing == routing);
                var version = ElasticDocumentVersion.Parse(document.Version ?? "3:10");
                response = new
                {
                    _index = $"{document.Index}-v1",
                    _id = id,
                    _routing = routing,
                    found = true,
                    _seq_no = version.SequenceNumber,
                    _primary_term = version.PrimaryTerm,
                    _source = new { id, index = document.Index, routing, value = document.Value }
                };
            }

            byte[] bytes = JsonSerializer.SerializeToUtf8Bytes(response);
            using var stream = new MemoryStream(bytes);
            return ResponseFactory.Create<TResponse>(endpoint, boundConfiguration, postData, null, statusCode, _headers, stream, "application/json", bytes.Length, null, null);
        }

        private object CreateBulkResponse(BoundConfiguration boundConfiguration, PostData? postData)
        {
            Assert.NotNull(postData);
            using var requestStream = new MemoryStream();
            postData.Write(requestStream, boundConfiguration.ConnectionSettings, false);
            requestStream.Position = 0;
            using var reader = new StreamReader(requestStream);
            var items = new List<object>();
            while (reader.ReadLine() is { } line)
            {
                using var metadata = JsonDocument.Parse(line);
                var operation = Assert.Single(metadata.RootElement.EnumerateObject());
                string index = operation.Value.GetProperty("_index").GetString()!;
                string id = operation.Value.GetProperty("_id").GetString()!;
                string routing = operation.Value.GetProperty("routing").GetString()!;
                Writes.Add((index, id, routing));
                string? documentJson = reader.ReadLine();
                Assert.NotNull(documentJson);
                using var source = JsonDocument.Parse(documentJson);
                string value = source.RootElement.GetProperty("value").GetString()!;
                if (operation.Value.TryGetProperty("pipeline", out var pipeline) && pipeline.GetString() is "lowercase-value")
                {
                    value = value.ToLowerInvariant();
                    PipelineWrites++;
                }
                _documents[(index, id, routing)] = new TestDocument { Id = id, Index = index, Routing = routing, Value = value, Version = $"3:{Writes.Count * 10}" };
                items.Add(new Dictionary<string, object>
                {
                    [operation.Name] = new
                    {
                        _index = $"{index}-v1",
                        _id = id,
                        status = operation.Name is "create" ? 201 : 200,
                        result = operation.Name is "create" ? "created" : "updated",
                        _seq_no = Writes.Count * 10,
                        _primary_term = 3
                    }
                });
            }

            return new { took = 1, errors = false, items };
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
