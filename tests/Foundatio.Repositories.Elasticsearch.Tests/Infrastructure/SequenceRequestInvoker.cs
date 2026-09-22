using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Transport;

namespace Foundatio.Repositories.Elasticsearch.Tests.Infrastructure;

/// <summary>Returns ordered transport responses without contacting Elasticsearch.</summary>
internal sealed class SequenceRequestInvoker : IRequestInvoker
{
    private readonly InMemoryRequestInvoker[] _responses;
    private int _position = -1;

    public SequenceRequestInvoker(params (int StatusCode, string Body)[] responses)
    {
        ArgumentNullException.ThrowIfNull(responses);
        if (responses.Length is 0)
            throw new ArgumentException("At least one response is required.", nameof(responses));

        _responses = responses.Select(response => new InMemoryRequestInvoker(
            Encoding.UTF8.GetBytes(response.Body), response.StatusCode,
            exception: null, contentType: "application/json",
            headers: new Dictionary<string, IEnumerable<string>> { ["x-elastic-product"] = ["Elasticsearch"] })).ToArray();
    }

    public ResponseFactory ResponseFactory => _responses[Math.Clamp(Volatile.Read(ref _position), 0, _responses.Length - 1)].ResponseFactory;

    private InMemoryRequestInvoker Next()
    {
        int position = Interlocked.Increment(ref _position);
        if (position >= _responses.Length)
            throw new InvalidOperationException("The request exceeded the supplied response sequence.");

        return _responses[position];
    }

    public TResponse Request<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData)
        where TResponse : TransportResponse, new()
        => Next().Request<TResponse>(endpoint, boundConfiguration, postData);

    public Task<TResponse> RequestAsync<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData, CancellationToken cancellationToken = default)
        where TResponse : TransportResponse, new()
        => Next().RequestAsync<TResponse>(endpoint, boundConfiguration, postData, cancellationToken);

    public void Dispose() { }
}
