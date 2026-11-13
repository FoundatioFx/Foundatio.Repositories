using System;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Transport;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class FaultInjectingRequestInvokerTests
{
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
