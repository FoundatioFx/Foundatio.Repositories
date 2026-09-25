using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
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

    private sealed class TestRepository : ElasticReadOnlyRepositoryBase<Identity>
    {
        public TestRepository(IIndex index) : base(index) { }
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

        public void Dispose()
        {
            Repository.Dispose();
            _configuration.Dispose();
            _invoker.Dispose();
            Cache.Dispose();
        }
    }
}
