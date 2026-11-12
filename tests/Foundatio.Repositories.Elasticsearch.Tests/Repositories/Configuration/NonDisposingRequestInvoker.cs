using System;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Transport;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration;

/// <summary>
/// Wraps an <see cref="IRequestInvoker"/> so it can be shared by many short-lived test clients without being disposed
/// when a client's settings are disposed. Sharing keeps the HTTP connection pool bounded across the test run.
/// </summary>
public sealed class NonDisposingRequestInvoker(IRequestInvoker inner) : IRequestInvoker
{
    public ResponseFactory ResponseFactory => inner.ResponseFactory;

    public Task<TResponse> RequestAsync<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData, CancellationToken cancellationToken)
        where TResponse : TransportResponse, new()
    {
        return inner.RequestAsync<TResponse>(endpoint, boundConfiguration, postData, cancellationToken);
    }

    public TResponse Request<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData)
        where TResponse : TransportResponse, new()
    {
        return inner.Request<TResponse>(endpoint, boundConfiguration, postData);
    }

    public void Dispose() { }
}
