using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Core.Bulk;
using Elastic.Clients.Elasticsearch.Core.MGet;
using Elastic.Transport;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class FaultInjectingRequestInvokerTests
{
    private const string BulkResponseBody = """{"took":1,"errors":false,"items":[{"delete":{"_index":"test","_id":"one","status":200,"result":"deleted"}}]}""";
    private const string MultiGetResponseBody = """{"docs":[{"_index":"test","_id":"one","found":true,"_source":{"name":"one"}}]}""";

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Rewrite_WithElasticsearchResponse_PreservesProductCheckAndRequestedHeaders(bool bulk)
    {
        // Arrange
        var headers = new Dictionary<string, IEnumerable<string>>(StringComparer.OrdinalIgnoreCase)
        {
            ["X-Elastic-Product"] = ["Elasticsearch"],
            ["X-Request-Id"] = ["request-one"]
        };
        using var invoker = new FaultInjectingRequestInvoker(new InMemoryRequestInvoker(Encoding.UTF8.GetBytes(bulk ? BulkResponseBody : MultiGetResponseBody), 200, null, "application/json; charset=utf-8", headers));
        var client = new ElasticsearchClient(new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://localhost:9200")), invoker)
            .DisableDirectStreaming()
            .ResponseHeadersToParse(new HeadersList("X-Request-Id")));

        // Act
        TransportResponse response;
        if (bulk)
        {
            invoker.FailFirstItemOfNextBulk(409, "version_conflict_engine_exception", "révision différente");
            var result = await client.BulkAsync(new BulkRequest { Operations = [new BulkDeleteOperation("one") { Index = "test" }] }, TestContext.Current.CancellationToken);
            Assert.Equal("révision différente", Assert.Single(result.Items).Error?.Reason);
            response = result;

            var next = await client.BulkAsync(new BulkRequest { Operations = [new BulkDeleteOperation("one") { Index = "test" }] }, TestContext.Current.CancellationToken);
            Assert.True(next.IsValidResponse);
        }
        else
        {
            invoker.FailFirstDocOfNextMultiGet("no_shard_available_action_exception", "shard unavailable");
            var result = await client.MultiGetAsync<object>(new MultiGetRequestDescriptor().Docs([new MultiGetOperation("one") { Index = "test" }]), TestContext.Current.CancellationToken);
            Assert.Equal("no_shard_available_action_exception", Assert.Single(result.ToMultiGetItems()).ErrorType);
            response = result;
        }

        // Assert
        Assert.Equal(200, response.ApiCallDetails.HttpStatusCode);
        Assert.Equal("application/json; charset=utf-8", response.ApiCallDetails.ResponseContentType);
        Assert.True(response.ApiCallDetails.TryGetHeader("X-Elastic-Product", out var products));
        Assert.Equal("Elasticsearch", Assert.Single(products));
        Assert.True(response.ApiCallDetails.TryGetHeader("X-Request-Id", out var requestIds));
        Assert.Equal("request-one", Assert.Single(requestIds));
    }

    [Fact]
    public async Task RespondNext_WithElasticsearchQueryResponse_AllowsFreshClientProductCheck()
    {
        // Arrange
        using var invoker = new FaultInjectingRequestInvoker(new InMemoryRequestInvoker());
        invoker.RespondNext(1, 200, """{"took":1,"timed_out":false,"total":3,"deleted":3,"version_conflicts":0,"failures":[]}""", HttpMethod.POST, "_delete_by_query", elasticsearchResponse: true);
        var client = new ElasticsearchClient(new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://localhost:9200")), invoker));

        // Act
        var response = await client.DeleteByQueryAsync(new DeleteByQueryRequest("test"), TestContext.Current.CancellationToken);

        // Assert
        Assert.True(response.IsValidResponse);
        Assert.Equal(3, response.Deleted);
    }

    [Fact]
    public async Task Rewrite_WithoutProductIdentity_DoesNotInventElasticsearchIdentity()
    {
        // Arrange
        using var invoker = new FaultInjectingRequestInvoker(new InMemoryRequestInvoker(Encoding.UTF8.GetBytes(BulkResponseBody), 200));
        invoker.FailFirstItemOfNextBulk(409, "version_conflict_engine_exception", "injected conflict");
        var client = new ElasticsearchClient(new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://localhost:9200")), invoker).DisableDirectStreaming());

        // Act & Assert
        await Assert.ThrowsAsync<UnsupportedProductException>(() => client.BulkAsync(new BulkRequest { Operations = [new BulkDeleteOperation("one") { Index = "test" }] }, TestContext.Current.CancellationToken));
    }

    [Theory]
    [InlineData(200)]
    [InlineData(503)]
    public async Task RespondNext_WithRawGatewayResponse_DoesNotInventElasticsearchIdentity(int status)
    {
        // Arrange
        using var invoker = new FaultInjectingRequestInvoker(new InMemoryRequestInvoker());
        invoker.RespondNext(1, status, """{"message":"upstream connection terminated"}""", HttpMethod.POST, "_update/doc");
        ITransport transport = new ElasticsearchClient(new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://localhost:9200")), invoker)).Transport;

        // Act
        var response = await transport.RequestAsync<StringResponse>(new EndpointPath(HttpMethod.POST, "test/_update/doc"), null, null,
            new RequestConfiguration { ResponseHeadersToParse = new HeadersList("X-Elastic-Product") }, TestContext.Current.CancellationToken);

        // Assert
        Assert.Equal(status, response.ApiCallDetails.HttpStatusCode);
        Assert.False(response.ApiCallDetails.TryGetHeader("X-Elastic-Product", out _));
        Assert.Equal("""{"message":"upstream connection terminated"}""", response.Body);
    }

    [Fact]
    public void Dispose_ReleasesOwnedConnectionResourcesOnce()
    {
        var inner = new CountingRequestInvoker();
        using var invoker = new FaultInjectingRequestInvoker(inner);

        invoker.Dispose();
        invoker.Dispose();

        Assert.Equal(1, inner.DisposeCount);
    }

    private sealed class CountingRequestInvoker : IRequestInvoker
    {
        private readonly InMemoryRequestInvoker _inner = new();
        public int DisposeCount { get; private set; }
        public ResponseFactory ResponseFactory => _inner.ResponseFactory;

        public TResponse Request<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData)
            where TResponse : TransportResponse, new()
        {
            return _inner.Request<TResponse>(endpoint, boundConfiguration, postData);
        }

        public Task<TResponse> RequestAsync<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData, CancellationToken cancellationToken)
            where TResponse : TransportResponse, new()
        {
            return _inner.RequestAsync<TResponse>(endpoint, boundConfiguration, postData, cancellationToken);
        }

        public void Dispose()
        {
            DisposeCount++;
            ((IDisposable)_inner).Dispose();
        }
    }
}
