using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Transport;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration;

/// <summary>
/// Forwards requests to a real invoker, but answers the first matching requests with an injected status code instead.
/// Used to test retries of transient failures (HTTP 429 and 503) without a misbehaving cluster.
/// </summary>
public sealed class FaultInjectingRequestInvoker(IRequestInvoker inner) : IRequestInvoker
{
    private readonly ConcurrentQueue<(Func<Endpoint, bool> Match, int StatusCode, string Body)> _faults = new();
    private int _injected;

    public ResponseFactory ResponseFactory => inner.ResponseFactory;

    /// <summary>Number of requests answered with an injected fault.</summary>
    public int InjectedCount => _injected;

    /// <summary>
    /// Fails the next <paramref name="count"/> requests whose method and path match with <paramref name="statusCode"/>.
    /// </summary>
    public void FailNext(int count, int statusCode, HttpMethod method, string pathContains)
    {
        string body = $$"""{"error":{"type":"injected_fault","reason":"injected {{statusCode}}"},"status":{{statusCode}}}""";
        for (int i = 0; i < count; i++)
            _faults.Enqueue((endpoint => endpoint.Method == method && endpoint.PathAndQuery.Contains(pathContains, StringComparison.Ordinal), statusCode, body));
    }

    private readonly ConcurrentQueue<(HttpMethod Method, string Path)> _requests = new();
    private Func<Task>? _beforeNextBulk;

    /// <summary>
    /// Runs <paramref name="action"/> once, right before the next <c>_bulk</c> request is sent. Use it to change documents
    /// after they were read and before they are written, which makes version conflicts deterministic.
    /// </summary>
    public void BeforeNextBulk(Func<Task> action) => _beforeNextBulk = action;

    /// <summary>Counts the requests sent (including injected faults) whose method and path match.</summary>
    public int CountRequests(HttpMethod method, string pathContains)
    {
        return _requests.Count(r => r.Method == method && r.Path.Contains(pathContains, StringComparison.Ordinal));
    }

    public async Task<TResponse> RequestAsync<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData, CancellationToken cancellationToken)
        where TResponse : TransportResponse, new()
    {
        _requests.Enqueue((endpoint.Method, endpoint.PathAndQuery));
        if (endpoint.PathAndQuery.Contains("_bulk", StringComparison.Ordinal) && Interlocked.Exchange(ref _beforeNextBulk, null) is { } beforeBulk)
            await beforeBulk().ConfigureAwait(false);

        if (TryTakeFault(endpoint, out var fault))
            return CreateFaultResponse<TResponse>(endpoint, boundConfiguration, postData, fault.StatusCode, fault.Body);

        return await inner.RequestAsync<TResponse>(endpoint, boundConfiguration, postData, cancellationToken).ConfigureAwait(false);
    }

    public TResponse Request<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData)
        where TResponse : TransportResponse, new()
    {
        if (TryTakeFault(endpoint, out var fault))
            return CreateFaultResponse<TResponse>(endpoint, boundConfiguration, postData, fault.StatusCode, fault.Body);

        return inner.Request<TResponse>(endpoint, boundConfiguration, postData);
    }

    private bool TryTakeFault(Endpoint endpoint, out (Func<Endpoint, bool> Match, int StatusCode, string Body) fault)
    {
        if (_faults.TryPeek(out fault) && fault.Match(endpoint) && _faults.TryDequeue(out fault))
        {
            Interlocked.Increment(ref _injected);
            return true;
        }

        return false;
    }

    private TResponse CreateFaultResponse<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData, int statusCode, string body)
        where TResponse : TransportResponse, new()
    {
        using var stream = new System.IO.MemoryStream(Encoding.UTF8.GetBytes(body));
        return ResponseFactory.Create<TResponse>(endpoint, boundConfiguration, postData, null, statusCode, null, stream, "application/json", body.Length, null, null);
    }

    public void Dispose() { }
}
