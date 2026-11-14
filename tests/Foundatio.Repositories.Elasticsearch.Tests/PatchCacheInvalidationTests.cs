using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
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
    public async Task PatchAsync_WhenSourceIdChanges_InvalidatesOriginalWriteLocations(bool multipleIds)
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
    }

    public sealed class TestDocument : IIdentity
    {
        public string Id { get; set; } = null!;
        public string Value { get; set; } = null!;
    }

    private sealed class TestRepository : ElasticRepositoryBase<TestDocument>
    {
        public TestRepository(TestConfiguration configuration) : base(new TestIndex(configuration))
        {
            DefaultPipeline = "patch-cache-pipeline";
            NotificationsEnabled = false;
        }

        public int DocumentInvalidations { get; private set; }

        public Task SeedAsync(string id) => Cache.SetAsync(id, "cached");
        public Task<bool> CacheContainsAsync(string id) => Cache.ExistsAsync(id);

        protected override Task InvalidateCacheAsync(IReadOnlyCollection<ModifiedDocument<TestDocument>> documents, ChangeType? changeType = null)
        {
            if (documents.Count > 0)
                DocumentInvalidations++;

            return base.InvalidateCacheAsync(documents, changeType);
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
        private static readonly Dictionary<string, IEnumerable<string>> _headers = new(StringComparer.OrdinalIgnoreCase) { ["X-Elastic-Product"] = ["Elasticsearch"] };
        public ResponseFactory ResponseFactory => _owner.ResponseFactory;

        public TResponse Request<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData)
            where TResponse : TransportResponse, new()
        {
            Assert.NotNull(postData);
            using var requestStream = new MemoryStream();
            postData.Write(requestStream, boundConfiguration.ConnectionSettings, false);
            requestStream.Position = 0;
            string body;
            if (endpoint.PathAndQuery.Contains("_mget", StringComparison.Ordinal))
            {
                using var request = JsonDocument.Parse(requestStream);
                var documents = request.RootElement.GetProperty("docs").EnumerateArray().Select(document => new
                {
                    _index = "patch-cache-tests",
                    _id = document.GetProperty("_id").GetString(),
                    _seq_no = 0,
                    _primary_term = 1,
                    found = true,
                    _source = new { id = document.GetProperty("_id").GetString(), value = "before" }
                }).ToArray();
                body = JsonSerializer.Serialize(new { docs = documents });
            }
            else
            {
                Assert.Contains("_bulk", endpoint.PathAndQuery, StringComparison.Ordinal);
                using var reader = new StreamReader(requestStream);
                var items = new List<object>();
                while (reader.ReadLine() is { } line)
                {
                    using var metadata = JsonDocument.Parse(line);
                    var index = metadata.RootElement.GetProperty("index");
                    items.Add(new { index = new { _index = "patch-cache-tests", _id = index.GetProperty("_id").GetString(), _seq_no = 1, _primary_term = 1, status = 200, result = "updated" } });
                    Assert.NotNull(reader.ReadLine());
                }

                body = JsonSerializer.Serialize(new { took = 1, errors = false, items });
            }

            byte[] bytes = Encoding.UTF8.GetBytes(body);
            using var responseStream = new MemoryStream(bytes);
            return ResponseFactory.Create<TResponse>(endpoint, boundConfiguration, postData, null, 200, _headers, responseStream, "application/json", bytes.Length, null, null);
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
