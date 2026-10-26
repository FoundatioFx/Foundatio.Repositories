using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Transport;
using Foundatio.Caching;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Indexes;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class MultiGetResponseValidationTests
{
    [Theory]
    [InlineData("""{"docs":[]}""")]
    [InlineData("""{"docs":[null]}""")]
    [InlineData("""{"docs":[{"_index":"identity","_id":"other","found":false}]}""")]
    [InlineData("""{"docs":[{"_index":"identity","_id":"other","error":{"type":"index_closed_exception","reason":"closed"}}]}""")]
    [InlineData("""{"docs":[{"_index":"identity","error":{"type":"index_closed_exception","reason":"closed"}}]}""")]
    [InlineData("""{"docs":[{"_index":"identity","_id":"requested","found":true}]}""")]
    [InlineData("""{"docs":[{"_index":"identity","_id":"requested","found":false,"_source":{"id":"requested"}}]}""")]
    public async Task RejectsMalformedResponsesBeforeCachingAsync(string body)
    {
        using var fixture = new Fixture(body);

        await Assert.ThrowsAsync<DocumentException>(() => fixture.Repository.GetByIdsAsync(["requested"], o => o.Cache().ThrowOnMultiGetErrors()));

        Assert.Equal(0, fixture.Cache.Count);
    }

    [Fact]
    public async Task RejectsDuplicateResponseIdsAsync()
    {
        using var fixture = new Fixture("""
            {"docs":[
                {"_index":"identity","_id":"first","found":false},
                {"_index":"identity","_id":"first","found":false}
            ]}
            """);

        await Assert.ThrowsAsync<DocumentException>(() => fixture.Repository.GetByIdsAsync(["first", "second"], o => o.Cache().ThrowOnMultiGetErrors()));

        Assert.Equal(0, fixture.Cache.Count);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PreservesNormalFoundAndMissingResultsAsync(bool strict)
    {
        using var fixture = new Fixture("""
            {"docs":[
                {"_index":"identity","_id":"present","found":true,"_source":{"id":"present"}},
                {"_index":"identity","_id":"missing","found":false}
            ]}
            """);

        var documents = await fixture.Repository.GetByIdsAsync(["present", "missing"], o => o.Cache().ThrowOnMultiGetErrors(strict));

        Assert.Equal("present", Assert.Single(documents).Id);
        Assert.Equal(2, fixture.Cache.Count);
    }

    [Fact]
    public async Task DoesNotRequireProjectedSourceIdAsync()
    {
        using var fixture = new Fixture("""
            {"docs":[{"_index":"identity","_id":"projected","found":true,"_source":{}}]}
            """);

        var documents = await fixture.Repository.GetByIdsAsync(["projected"], o => o.ThrowOnMultiGetErrors());

        Assert.Null(Assert.Single(documents).Id);
    }

    [Fact]
    public async Task FailurePreservesExistingCacheWithoutCachingOtherHitsAsync()
    {
        using var fixture = new Fixture("""
            {"docs":[
                {"_index":"identity","_id":"present","found":true,"_source":{"id":"present"}},
                {"_index":"identity","_id":"failed","error":{"type":"index_closed_exception","reason":"closed"}}
            ]}
            """);
        var cachedDocument = new Identity { Id = "cached" };
        await fixture.Cache.SetAsync<ICollection<FindHit<Identity>>>("Identity:cached", [new FindHit<Identity>("cached", cachedDocument, 0)]);

        var exception = await Assert.ThrowsAsync<DocumentException>(() => fixture.Repository.GetByIdsAsync(["cached", "present", "failed"], o => o.Cache().ThrowOnMultiGetErrors()));

        Assert.Contains("failed", exception.Message);
        Assert.Contains("index_closed_exception", exception.Message);
        Assert.Equal(1, fixture.Cache.Count);
        var cached = await fixture.Cache.GetAsync<ICollection<FindHit<Identity>>>("Identity:cached");
        Assert.True(cached.HasValue);
        Assert.Equal(cachedDocument, Assert.Single(cached.Value).Document);
    }

    [Fact]
    public async Task ExplicitCacheBypassReachesElasticsearchAsync()
    {
        using var fixture = new Fixture("""
            {"docs":[{"_index":"identity","_id":"cached","error":{"type":"index_closed_exception","reason":"closed"}}]}
            """);
        var document = new Identity { Id = "cached" };
        await fixture.Cache.SetAsync<ICollection<FindHit<Identity>>>("Identity:cached", [new FindHit<Identity>("cached", document, 0)]);

        Assert.Equal(document, Assert.Single(await fixture.Repository.GetByIdsAsync(["cached"], o => o.Cache().ThrowOnMultiGetErrors())));
        await Assert.ThrowsAsync<DocumentException>(() => fixture.Repository.GetByIdsAsync(["cached"], o => o.Cache(false).ReadCache(false).ThrowOnMultiGetErrors()));
        Assert.Equal(1, fixture.Cache.Count);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RequestLevelFailuresAlwaysThrowAsync(bool strict)
    {
        using var fixture = new Fixture("""
            {"error":{"type":"internal_server_error","reason":"scripted failure"},"status":500}
            """, 500);

        await Assert.ThrowsAsync<DocumentException>(() => fixture.Repository.GetByIdsAsync(["requested"], o => o.Cache().ThrowOnMultiGetErrors(strict)));

        Assert.Equal(0, fixture.Cache.Count);
    }

    [Fact]
    public void ItemErrorCollectionRejectsNullResponse()
    {
        Assert.Throws<ArgumentNullException>(() => ElasticIndexExtensions.GetItemErrors<Identity>(null!));
    }

    [Fact]
    public void StrictOptionDefaultsToFalseAndCanBeDisabled()
    {
        var options = new CommandOptions();
        Assert.False(options.ShouldThrowOnMultiGetErrors());
        Assert.Same(options, options.ThrowOnMultiGetErrors());
        Assert.True(options.ShouldThrowOnMultiGetErrors());
        Assert.Same(options, options.ThrowOnMultiGetErrors(false));
        Assert.False(options.ShouldThrowOnMultiGetErrors());
    }

    public static IEnumerable<object[]> IncompleteFallbackCases()
    {
        foreach (bool timeout in new[] { false, true })
            foreach (bool multiGet in new[] { false, true })
                foreach (bool laterPage in new[] { false, true })
                    foreach (bool strict in new[] { false, true })
                        foreach (bool snapshot in new[] { false, true })
                            foreach (bool keyed in new[] { false, true })
                                yield return [timeout, multiGet, laterPage, strict, snapshot, keyed];
    }

    [Theory]
    [MemberData(nameof(IncompleteFallbackCases))]
    public async Task IncompleteFallback_PreservesStrictAndPermissiveCacheBehaviorAsync(bool timeout, bool multiGet, bool laterPage, bool strict, bool snapshot, bool keyed)
    {
        using var cache = new InMemoryCacheClient();
        using var invoker = new FallbackRequestInvoker(timeout, laterPage, snapshot: snapshot);
        using var configuration = new TestConfiguration(invoker, cache);
        using var repository = new TestRepository(new IdentityIndex(configuration) { HasMultipleIndexes = multiGet }, !multiGet);
        var cachedDocument = new Identity { Id = "cached" };
        await cache.SetAsync<ICollection<FindHit<Identity>>>("Identity:cached", [new FindHit<Identity>("cached", cachedDocument, 0)]);
        var options = new CommandOptions().Cache(keyed ? "batch" : null).ReadCache(false).ThrowOnMultiGetErrors(strict);
        if (keyed)
            await cache.SetAsync<ICollection<FindHit<Identity>>>("Identity:batch", [new FindHit<Identity>("cached", cachedDocument, 0)]);
        if (snapshot)
            options.SnapshotPaging();
        var ids = new Ids("present", "present2", "missing");

        if (strict)
        {
            var exception = await Assert.ThrowsAsync<DocumentException>(() => repository.GetByIdsAsync(ids, options));
            Assert.Contains(timeout ? "timed out" : "failed shards", exception.Message);
            Assert.Equal(keyed ? 2 : 1, cache.Count);
            if (keyed)
            {
                var batch = await cache.GetAsync<ICollection<FindHit<Identity>>>("Identity:batch");
                Assert.Equal(cachedDocument, Assert.Single(batch.Value).Document);
            }
        }
        else
        {
            var documents = await repository.GetByIdsAsync(ids, options);
            Assert.Equal(laterPage ? 2 : 1, documents.Count);
            var missing = await cache.GetAsync<ICollection<FindHit<Identity>>>("Identity:missing");
            Assert.True(missing.HasValue);
            Assert.Null(Assert.Single(missing.Value).Document);
            Assert.Equal(keyed ? 5 : 4, cache.Count);
        }

        var cached = await cache.GetAsync<ICollection<FindHit<Identity>>>("Identity:cached");
        Assert.Equal(cachedDocument, Assert.Single(cached.Value).Document);
        Assert.Equal(multiGet ? 1 : 0, invoker.MultiGetCount);
        Assert.Equal(laterPage ? 2 : 1, invoker.SearchCount);
        Assert.True(options.ShouldUseCache());
        Assert.False(options.ShouldReadCache());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CompleteFallback_StrictModeCachesHitsAndOrdinaryMissesAsync(bool multiGet)
    {
        using var cache = new InMemoryCacheClient();
        using var invoker = new FallbackRequestInvoker(false, false, complete: true);
        using var configuration = new TestConfiguration(invoker, cache);
        using var repository = new TestRepository(new IdentityIndex(configuration) { HasMultipleIndexes = multiGet }, !multiGet);
        var ids = new Ids("present", "missing");

        var documents = await repository.GetByIdsAsync(ids, o => o.Cache().ThrowOnMultiGetErrors());

        Assert.Equal("present", Assert.Single(documents).Id);
        Assert.Equal(2, cache.Count);
        var missing = await cache.GetAsync<ICollection<FindHit<Identity>>>("Identity:missing");
        Assert.True(missing.HasValue);
        Assert.Null(Assert.Single(missing.Value).Document);
        Assert.Equal(multiGet ? 1 : 0, invoker.MultiGetCount);
        Assert.Equal(1, invoker.SearchCount);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task DirectFind_PublicMultiGetOptionDoesNotChangePartialSearchBehaviorAsync(bool strict)
    {
        using var cache = new InMemoryCacheClient();
        using var invoker = new FallbackRequestInvoker(true, false);
        using var configuration = new TestConfiguration(invoker, cache);
        using var repository = new TestRepository(new IdentityIndex(configuration));

        Assert.Single((await repository.FindAsync(new RepositoryQuery<Identity>(), new CommandOptions().ThrowOnMultiGetErrors(strict))).Documents);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task AsyncFallbackConverters_RejectIncompleteResponsesOnlyForInternalFlagAsync(bool timeout, bool get)
    {
        string body = JsonSerializer.Serialize(new
        {
            id = "synthetic-async",
            is_running = false,
            is_partial = false,
            response = new
            {
                took = 1,
                timed_out = timeout,
                _shards = new { total = 2, successful = timeout ? 2 : 1, skipped = 0, failed = timeout ? 0 : 1 },
                hits = new { total = new { value = 0, relation = "eq" }, hits = Array.Empty<object>() }
            }
        });
        using var fixture = new Fixture(body);
        var options = new CommandOptions().ThrowOnMultiGetErrors();
        if (get)
        {
            var response = await fixture.Configuration.Client.AsyncSearch.GetAsync<Identity>("synthetic-async", cancellationToken: TestContext.Current.CancellationToken);
            Assert.Empty(response.ToFindResults(options, fixture.Configuration.Serializer).Documents);
            options.BuildOption("@RequireCompleteSearchResults", true);
            Assert.Throws<DocumentException>(() => response.ToFindResults(options, fixture.Configuration.Serializer));
        }
        else
        {
            var response = await fixture.Configuration.Client.AsyncSearch.SubmitAsync<Identity>(cancellationToken: TestContext.Current.CancellationToken);
            Assert.Empty(response.ToFindResults(options, fixture.Configuration.Serializer).Documents);
            options.BuildOption("@RequireCompleteSearchResults", true);
            Assert.Throws<DocumentException>(() => response.ToFindResults(options, fixture.Configuration.Serializer));
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FallbackSearch_WhenTargetIsMissing_StrictModeRejectsEmptyResultAsync(bool scroll)
    {
        using var fixture = new Fixture("""
            {"error":{"type":"search_context_missing_exception","reason":"search target unavailable"},"status":404}
            """, 404);
        var options = new CommandOptions();
        Func<FindResults<Identity>> convert;
        if (scroll)
        {
            var response = await fixture.Configuration.Client.ScrollAsync<Identity>(new ScrollRequest { ScrollId = "missing" }, TestContext.Current.CancellationToken);
            convert = () => response.ToFindResults(options, fixture.Configuration.Serializer);
        }
        else
        {
            var response = await fixture.Configuration.Client.SearchAsync<Identity>(cancellationToken: TestContext.Current.CancellationToken);
            convert = () => response.ToFindResults(options, fixture.Configuration.Serializer);
        }

        Assert.Empty(convert().Documents);
        options.BuildOption("@RequireCompleteSearchResults", true);
        Assert.Throws<DocumentException>(convert);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AsyncFallback_WhenTargetIsMissing_StrictModeRejectsEmptyResultAsync(bool get)
    {
        using var fixture = new Fixture("""
            {"error":{"type":"resource_not_found_exception","reason":"async search unavailable"},"status":404}
            """, 404);
        var options = new CommandOptions();
        Func<FindResults<Identity>> convert;
        if (get)
        {
            var response = await fixture.Configuration.Client.AsyncSearch.GetAsync<Identity>("missing", cancellationToken: TestContext.Current.CancellationToken);
            convert = () => response.ToFindResults(options, fixture.Configuration.Serializer);
        }
        else
        {
            var response = await fixture.Configuration.Client.AsyncSearch.SubmitAsync<Identity>(cancellationToken: TestContext.Current.CancellationToken);
            convert = () => response.ToFindResults(options, fixture.Configuration.Serializer);
        }

        Assert.Empty(convert().Documents);
        options.BuildOption("@RequireCompleteSearchResults", true);
        Assert.Throws<DocumentException>(convert);
    }

    private sealed class FallbackRequestInvoker(bool timeout, bool laterPage, bool complete = false, bool snapshot = false)
        : InMemoryRequestInvoker(null, headers: new Dictionary<string, IEnumerable<string>>(StringComparer.OrdinalIgnoreCase) { ["x-elastic-product"] = ["Elasticsearch"] }), IRequestInvoker
    {
        public int MultiGetCount { get; private set; }
        public int SearchCount { get; private set; }

        TResponse IRequestInvoker.Request<TResponse>(Endpoint endpoint, BoundConfiguration configuration, PostData? postData)
        {
            if (typeof(TResponse) != typeof(GetMappingResponse))
                throw new InvalidOperationException($"Unexpected synchronous request: {endpoint.Uri}");
            return BuildResponse<TResponse>(endpoint, configuration, postData, Encoding.UTF8.GetBytes("""{"identity":{"mappings":{"properties":{"id":{"type":"keyword"}}}}}"""));
        }

        Task<TResponse> IRequestInvoker.RequestAsync<TResponse>(Endpoint endpoint, BoundConfiguration configuration, PostData? postData, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            byte[] body;
            if (typeof(TResponse) == typeof(MultiGetResponse<Identity>))
            {
                MultiGetCount++;
                body = Encoding.UTF8.GetBytes(complete
                    ? """{"docs":[{"_index":"identity","_id":"present","found":false},{"_index":"identity","_id":"missing","found":false}]}"""
                    : """{"docs":[{"_index":"identity","_id":"present","found":false},{"_index":"identity","_id":"present2","found":false},{"_index":"identity","_id":"missing","found":false}]}""");
            }
            else if (typeof(TResponse) == typeof(SearchResponse<Identity>) || typeof(TResponse) == typeof(ScrollResponse<Identity>))
            {
                SearchCount++;
                if (SearchCount > (laterPage ? 2 : 1))
                    throw new InvalidOperationException("Unexpected extra search page.");
                bool failed = !complete && (!laterPage || SearchCount == 2);
                string[] hitIds = laterPage ? (SearchCount == 1 ? ["present", "present2"] : []) : ["present"];
                body = JsonSerializer.SerializeToUtf8Bytes(new
                {
                    _scroll_id = snapshot ? "synthetic-scroll" : null,
                    took = 1,
                    timed_out = failed && timeout,
                    _shards = new { total = 2, successful = failed && !timeout ? 1 : 2, skipped = 0, failed = failed && !timeout ? 1 : 0 },
                    hits = new { total = new { value = hitIds.Length, relation = "eq" }, hits = Array.ConvertAll(hitIds, id => new { _index = "identity", _id = id, _source = new { id } }) }
                });
            }
            else if (typeof(TResponse) == typeof(ClearScrollResponse))
                body = Encoding.UTF8.GetBytes("""{"succeeded":true,"num_freed":1}""");
            else
                throw new InvalidOperationException($"Unexpected response type: {typeof(TResponse).Name}, request: {endpoint.Uri}");

            return BuildResponseAsync<TResponse>(endpoint, configuration, postData, cancellationToken, body);
        }
    }

    private sealed class TestRepository : ElasticReadOnlyRepositoryBase<Identity>
    {
        public TestRepository(IIndex index, bool hasParent = false) : base(index)
        {
            HasParent = hasParent;
            MaxPageLimit = 2;
        }
    }

    private sealed class TestConfiguration : ElasticConfiguration
    {
        private readonly IRequestInvoker _invoker;

        public TestConfiguration(IRequestInvoker invoker, ICacheClient cache) : base(cacheClient: cache)
        {
            _invoker = invoker;
        }

        protected override ElasticsearchClient CreateElasticClient()
        {
            var settings = new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://localhost:9200")), _invoker)
                .DisableDirectStreaming()
                .DisablePing()
                .MaximumRetries(0);
            return new ElasticsearchClient(settings);
        }
    }

    private sealed class Fixture : IDisposable
    {
        private readonly IRequestInvoker _invoker;
        private readonly TestConfiguration _configuration;

        public Fixture(string body, int statusCode = 200)
        {
            Cache = new InMemoryCacheClient();
            _invoker = new InMemoryRequestInvoker(Encoding.UTF8.GetBytes(body), statusCode,
                headers: new Dictionary<string, IEnumerable<string>>(StringComparer.OrdinalIgnoreCase) { ["x-elastic-product"] = ["Elasticsearch"] });
            _configuration = new TestConfiguration(_invoker, Cache);
            Repository = new TestRepository(new IdentityIndex(_configuration));
        }

        public InMemoryCacheClient Cache { get; }
        public TestRepository Repository { get; }
        public TestConfiguration Configuration => _configuration;

        public void Dispose()
        {
            Repository.Dispose();
            _configuration.Dispose();
            _invoker.Dispose();
            Cache.Dispose();
        }
    }
}
