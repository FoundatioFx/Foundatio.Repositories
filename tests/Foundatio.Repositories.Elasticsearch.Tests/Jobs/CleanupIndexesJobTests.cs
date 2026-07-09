using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Foundatio.Caching;
using Foundatio.Lock;
using Foundatio.Messaging;
using Foundatio.Repositories.Elasticsearch.Jobs;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests.Jobs;

public sealed class CleanupIndexesJobTests
{
    [Fact]
    public async Task RunAsync_WhenIndexListRequestSucceeds_PreservesRequestContract()
    {
        using var invoker = new RecordingRequestInvoker(Encoding.UTF8.GetBytes("{}"));
        var client = CreateClient(invoker);
        var job = CreateJob(client);
        using var cancellationTokenSource = new CancellationTokenSource();

        var result = await job.RunAsync(cancellationTokenSource.Token);

        Assert.True(result.IsSuccess);
        Assert.NotNull(invoker.Endpoint);
        Assert.Contains("features=aliases", invoker.Endpoint.PathAndQuery);
        Assert.Contains("include_defaults=false", invoker.Endpoint.PathAndQuery);
        Assert.Contains("ignore_unavailable=true", invoker.Endpoint.PathAndQuery);
        Assert.Equal(TimeSpan.FromMinutes(5), invoker.BoundConfiguration?.RequestTimeout);
        Assert.Equal(cancellationTokenSource.Token, invoker.CancellationToken);
    }

    [Fact]
    public async Task RunAsync_WhenIndexListRequestFails_ReturnsFailedResult()
    {
        const string response = """
            {
              "error": {
                "type": "illegal_argument_exception",
                "reason": "index metadata unavailable"
              },
              "status": 400
            }
            """;
        using var invoker = new RecordingRequestInvoker(Encoding.UTF8.GetBytes(response), statusCode: 400);
        var client = CreateClient(invoker);
        var job = CreateJob(client);

        var result = await job.RunAsync(TestContext.Current.CancellationToken);

        Assert.False(result.IsSuccess);
        Assert.Contains("Failed to retrieve list of indexes", result.Message);
    }

    private static ElasticsearchClient CreateClient(IRequestInvoker invoker)
    {
        var settings = new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://localhost:9200")), invoker);
        return new ElasticsearchClient(settings);
    }

    private static CleanupIndexesJob CreateJob(ElasticsearchClient client)
    {
        var lockProvider = new CacheLockProvider(new InMemoryCacheClient(), new InMemoryMessageBus(), NullLoggerFactory.Instance);
        return new CleanupIndexesJob(client, lockProvider, TimeProvider.System, NullLoggerFactory.Instance);
    }

    private sealed class RecordingRequestInvoker : IRequestInvoker
    {
        private readonly InMemoryRequestInvoker _inner;

        public RecordingRequestInvoker(byte[] responseBody, int statusCode = 200)
        {
            var headers = new Dictionary<string, IEnumerable<string>> { ["x-elastic-product"] = ["Elasticsearch"] };
            _inner = new InMemoryRequestInvoker(responseBody, statusCode, headers: headers);
        }

        public ResponseFactory ResponseFactory => _inner.ResponseFactory;
        public Endpoint? Endpoint { get; private set; }
        public BoundConfiguration? BoundConfiguration { get; private set; }
        public CancellationToken CancellationToken { get; private set; }

        public TResponse Request<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData) where TResponse : TransportResponse, new()
        {
            Record(endpoint, boundConfiguration, default);
            return _inner.Request<TResponse>(endpoint, boundConfiguration, postData);
        }

        public Task<TResponse> RequestAsync<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData, CancellationToken cancellationToken) where TResponse : TransportResponse, new()
        {
            Record(endpoint, boundConfiguration, cancellationToken);
            return _inner.RequestAsync<TResponse>(endpoint, boundConfiguration, postData, cancellationToken);
        }

        public void Dispose()
        {
            ((IDisposable)_inner).Dispose();
        }

        private void Record(Endpoint endpoint, BoundConfiguration boundConfiguration, CancellationToken cancellationToken)
        {
            Endpoint = endpoint;
            BoundConfiguration = boundConfiguration;
            CancellationToken = cancellationToken;
        }
    }
}
