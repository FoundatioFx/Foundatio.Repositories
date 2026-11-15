using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Transport;
using Foundatio.Caching;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Models;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class PatchCacheInvalidationTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PatchAsync_WithClientSideSuccess_InvalidatesEachDocumentOnce(bool multipleIds)
    {
        using var cache = new RecordingCache();
        using var configuration = new TestConfiguration(cache);
        using var repository = new TestRepository(configuration);
        var patch = new PartialPatch(new { value = "patched" });

        if (multipleIds)
            Assert.Equal(2, await repository.PatchAsync(new Ids("one", "two"), patch, o => o.Retry(0)));
        else
            Assert.True(await repository.PatchAsync(new Id("one"), patch, o => o.Retry(0)));

        Assert.Equal(1, repository.DocumentInvalidations);
        var keys = Assert.Single(cache.Removals);
        Assert.Equal(multipleIds ? 2 : 1, keys.Length);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PatchAsync_WhenSourceIdChanges_InvalidatesAndNotifiesOriginalWriteLocations(bool multipleIds)
    {
        using var cache = new RecordingCache();
        using var configuration = new TestConfiguration(cache);
        using var repository = new TestRepository(configuration);
        await repository.SeedAsync("one");
        if (multipleIds)
            await repository.SeedAsync("two");
        await repository.SeedAsync("changed-source-id");
        var patch = new PartialPatch(new { id = "changed-source-id", value = "patched" });

        if (multipleIds)
            Assert.Equal(2, await repository.PatchAsync(new Ids("one", "two"), patch, options => options.Retry(0)));
        else
            Assert.True(await repository.PatchAsync(new Id("one"), patch, options => options.Retry(0)));

        Assert.False(await repository.CacheContainsAsync("one"));
        if (multipleIds)
            Assert.False(await repository.CacheContainsAsync("two"));
        Assert.False(await repository.CacheContainsAsync("changed-source-id"));
        Assert.Equal(1, repository.DocumentInvalidations);
        Assert.Equal(multipleIds ? new[] { "one", "two" } : ["one"], repository.PublishedIds);
    }

    [Theory]
    [InlineData(false, false, false)]
    [InlineData(false, false, true)]
    [InlineData(false, true, false)]
    [InlineData(false, true, true)]
    [InlineData(true, false, false)]
    [InlineData(true, false, true)]
    [InlineData(true, true, false)]
    [InlineData(true, true, true)]
    public async Task PatchAsync_WhenChangeHandlerReadsOrThrows_InvalidatesWriteLocationsBeforeTheHandler(bool sourceIdChanges, bool multipleIds, bool handlerThrows)
    {
        using var cache = new RecordingCache();
        using var configuration = new TestConfiguration(cache);
        using var repository = new TestRepository(configuration, usePipeline: sourceIdChanges);
        string[] ids = multipleIds ? ["one", "two"] : ["one"];
        foreach (string id in ids)
        {
            var cached = await repository.GetByIdAsync(id, options => options.Cache());
            Assert.NotNull(cached);
            Assert.Equal("before", cached.Value);
        }

        var observedValues = new List<string>();
        repository.DocumentsChanged.AddHandler(async (_, _) =>
        {
            foreach (string id in ids)
            {
                var current = await repository.GetByIdAsync(id, options => options.Cache());
                Assert.NotNull(current);
                observedValues.Add(current.Value);
            }

            if (handlerThrows)
                throw new InvalidOperationException("Post-patch handler failed.");
        });
        var patch = sourceIdChanges
            ? new PartialPatch(new { id = "changed-source-id", value = "patched" })
            : new PartialPatch(new { value = "patched" });

        var exception = await Record.ExceptionAsync(async () =>
        {
            if (multipleIds)
                Assert.Equal(ids.Length, await repository.PatchAsync(new Ids(ids), patch, options => options.Retry(0)));
            else
                Assert.True(await repository.PatchAsync(ids[0], patch, options => options.Retry(0)));
        });

        if (handlerThrows)
        {
            Assert.IsType<InvalidOperationException>(exception);
            Assert.Equal("Post-patch handler failed.", exception.Message);
        }
        else
        {
            Assert.Null(exception);
            Assert.Equal(ids, repository.PublishedIds);
        }
        Assert.Equal(ids.Length, observedValues.Count);
        Assert.All(observedValues, value => Assert.Equal("patched", value));
        foreach (string id in ids)
        {
            var current = await repository.GetByIdAsync(id, options => options.Cache());
            Assert.NotNull(current);
            Assert.Equal("patched", current.Value);
        }
        Assert.Equal(sourceIdChanges ? 1 : 0, repository.DocumentInvalidations);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task PatchAsync_WhenUpdatedIdsCallbackReadsOrThrows_PreservesCacheAndCallbackOrder(bool sourceIdChanges, bool callbackThrows)
    {
        using var cache = new RecordingCache();
        using var configuration = new TestConfiguration(cache);
        using var repository = new TestRepository(configuration, usePipeline: sourceIdChanges);
        string[] ids = ["one", "two"];
        foreach (string id in ids)
            Assert.NotNull(await repository.GetByIdAsync(id, options => options.Cache()));

        int changeEvents = 0;
        repository.DocumentsChanged.AddSyncHandler((_, _) => changeEvents++);
        int changeEventsAtCallback = 0;
        var notificationsAtCallback = new List<string?>();
        var observedReads = new List<Task<TestDocument?>>();
        var updatedIds = new List<string>();
        var patch = sourceIdChanges
            ? new PartialPatch(new { id = "changed-source-id", value = "patched" })
            : new PartialPatch(new { value = "patched" });

        long modified = await repository.PatchAsync(new Ids(ids), patch, options => options.Retry(0).UpdateCallback(changed =>
        {
            changeEventsAtCallback = changeEvents;
            notificationsAtCallback.AddRange(repository.PublishedIds);
            updatedIds.AddRange(changed);
            foreach (string id in changed)
                observedReads.Add(repository.GetByIdAsync(id, readOptions => readOptions.Cache()));
            if (callbackThrows)
                throw new InvalidOperationException("Updated IDs callback failed.");
        }));

        Assert.Equal(ids.Length, modified);
        Assert.Equal(1, changeEventsAtCallback);
        Assert.Equal(ids, notificationsAtCallback);
        Assert.Equal(ids, updatedIds);
        var observed = await Task.WhenAll(observedReads);
        Assert.Equal(ids.Length, observed.Length);
        Assert.All(observed, document =>
        {
            Assert.NotNull(document);
            Assert.Equal("patched", document.Value);
        });
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PatchAsync_WhenChangeHandlerMutatesSourceIds_NotifiesOriginalWriteLocations(bool multipleIds)
    {
        using var cache = new RecordingCache();
        using var configuration = new TestConfiguration(cache);
        using var repository = new TestRepository(configuration);
        repository.DocumentsChanged.AddSyncHandler((_, args) =>
        {
            foreach (var document in args.Documents)
                document.Value.Id = "unsaved-handler-id";
        });
        var patch = new PartialPatch(new { value = "patched" });

        if (multipleIds)
            Assert.Equal(2, await repository.PatchAsync(new Ids("one", "two"), patch, options => options.Retry(0)));
        else
            Assert.True(await repository.PatchAsync("one", patch, options => options.Retry(0)));

        Assert.Equal(multipleIds ? new[] { "one", "two" } : ["one"], repository.PublishedIds);
        Assert.Equal(1, repository.DocumentInvalidations);
    }

    public sealed class TestDocument : IIdentity
    {
        public string Id { get; set; } = null!;
        public string Value { get; set; } = null!;
    }

    private sealed class TestRepository : ElasticRepositoryBase<TestDocument>
    {
        public TestRepository(TestConfiguration configuration, bool usePipeline = true) : base(new TestIndex(configuration))
        {
            DefaultPipeline = usePipeline ? "patch-cache-pipeline" : null;
        }

        public int DocumentInvalidations { get; private set; }
        public List<string?> PublishedIds { get; } = [];

        public Task SeedAsync(string id) => Cache.SetAsync(id, "cached");
        public Task<bool> CacheContainsAsync(string id) => Cache.ExistsAsync(id);

        protected override Task InvalidateCacheAsync(IReadOnlyCollection<ModifiedDocument<TestDocument>> documents, ChangeType? changeType = null)
        {
            if (documents.Count > 0)
                DocumentInvalidations++;

            return base.InvalidateCacheAsync(documents, changeType);
        }

        protected override Task PublishMessageAsync(EntityChanged message, TimeSpan? delay = null)
        {
            PublishedIds.Add(message.Id);
            return Task.CompletedTask;
        }
    }

    private sealed class TestIndex(TestConfiguration configuration) : Index<TestDocument>(configuration, "patch-cache-tests")
    {
        public override Task EnsureIndexAsync(object? target) => Task.CompletedTask;
    }

    private sealed class RecordingCache : InMemoryCacheClient, ICacheClient
    {
        public List<string[]> Removals { get; } = [];

        Task<int> ICacheClient.RemoveAllAsync(IEnumerable<string>? keys)
        {
            var removedKeys = keys?.ToArray();
            if (removedKeys is { Length: > 0 })
                Removals.Add(removedKeys);

            return base.RemoveAllAsync(removedKeys);
        }
    }

    private sealed class TestConfiguration(RecordingCache cache) : ElasticConfiguration(cacheClient: cache)
    {
        private readonly PatchResponseInvoker _invoker = new();
        protected override IRequestInvoker CreateRequestInvoker() => _invoker;
    }

    private sealed class PatchResponseInvoker : IRequestInvoker
    {
        private readonly InMemoryRequestInvoker _owner = new();
        private readonly Dictionary<string, TestDocument> _documents = new(StringComparer.Ordinal)
        {
            ["one"] = new() { Id = "one", Value = "before" },
            ["two"] = new() { Id = "two", Value = "before" }
        };
        private static readonly Dictionary<string, IEnumerable<string>> _headers = new(StringComparer.OrdinalIgnoreCase) { ["X-Elastic-Product"] = ["Elasticsearch"] };
        public ResponseFactory ResponseFactory => _owner.ResponseFactory;

        public TResponse Request<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData)
            where TResponse : TransportResponse, new()
        {
            using var requestStream = new MemoryStream();
            postData?.Write(requestStream, boundConfiguration.ConnectionSettings, false);
            requestStream.Position = 0;
            object response;
            if (endpoint.PathAndQuery.Contains("_mget", StringComparison.Ordinal))
            {
                using var request = JsonDocument.Parse(requestStream);
                response = new { docs = request.RootElement.GetProperty("docs").EnumerateArray().Select(document => GetDocumentResponse(document.GetProperty("_id").GetString()!)).ToArray() };
            }
            else if (endpoint.PathAndQuery.Contains("_bulk", StringComparison.Ordinal))
            {
                using var reader = new StreamReader(requestStream);
                var items = new List<object>();
                while (reader.ReadLine() is { } line)
                {
                    using var metadata = JsonDocument.Parse(line);
                    var operation = Assert.Single(metadata.RootElement.EnumerateObject());
                    string id = operation.Value.GetProperty("_id").GetString()!;
                    string? documentJson = reader.ReadLine();
                    Assert.NotNull(documentJson);
                    using var source = JsonDocument.Parse(documentJson);
                    ApplyDocument(id, operation.Name is "update" ? source.RootElement.GetProperty("doc") : source.RootElement);
                    items.Add(new Dictionary<string, object>
                    {
                        [operation.Name] = new { _index = "patch-cache-tests", _id = id, _seq_no = 1, _primary_term = 1, status = 200, result = "updated" }
                    });
                }

                response = new { took = 1, errors = false, items };
            }
            else
            {
                var uri = new Uri(new Uri("http://localhost/"), endpoint.PathAndQuery);
                string id = Uri.UnescapeDataString(uri.Segments[^1]);
                if (endpoint.PathAndQuery.Contains("/_update/", StringComparison.Ordinal))
                {
                    using var request = JsonDocument.Parse(requestStream);
                    ApplyDocument(id, request.RootElement.GetProperty("doc"));
                    response = new { _index = "patch-cache-tests", _id = id, _seq_no = 1, _primary_term = 1, result = "updated", _shards = new { total = 1, successful = 1, failed = 0 } };
                }
                else
                {
                    Assert.Contains("/_doc/", endpoint.PathAndQuery, StringComparison.Ordinal);
                    response = GetDocumentResponse(id);
                }
            }

            byte[] bytes = JsonSerializer.SerializeToUtf8Bytes(response);
            using var responseStream = new MemoryStream(bytes);
            return ResponseFactory.Create<TResponse>(endpoint, boundConfiguration, postData, null, 200, _headers, responseStream, "application/json", bytes.Length, null, null);
        }

        private object GetDocumentResponse(string id)
        {
            var document = _documents[id];
            return new { _index = "patch-cache-tests", _id = id, _seq_no = 0, _primary_term = 1, found = true, _source = new { id = document.Id, value = document.Value } };
        }

        private void ApplyDocument(string id, JsonElement source)
        {
            var document = _documents[id];
            if (source.TryGetProperty("id", out var sourceId))
                document.Id = sourceId.GetString()!;
            if (source.TryGetProperty("value", out var value))
                document.Value = value.GetString()!;
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
