using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Transport;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration;

/// <summary>
/// Forwards requests to a real invoker, but can answer matching requests with injected statuses, bodies or transport
/// exceptions, or rewrite parts of real responses. Used to test retries and failure handling without a misbehaving cluster.
/// </summary>
public sealed class FaultInjectingRequestInvoker(IRequestInvoker inner) : IRequestInvoker
{
    private readonly ConcurrentQueue<Fault> _faults = new();
    private readonly ConcurrentQueue<(HttpMethod Method, string Path)> _requests = new();
    private readonly ConcurrentQueue<string> _failedBulkItemIds = new();
    private Func<Task>? _beforeNextBulk;
    private (int Status, string Type, string Reason)? _nextBulkItemFailure;
    private (string Type, string Reason)? _nextMultiGetItemFailure;
    private int _injected;
    private int _disposed;

    private readonly ConcurrentBag<BeforeRequest> _beforeRequests = [];

    private sealed class BeforeRequest(Func<Endpoint, bool> match, int occurrence, Func<Task> action)
    {
        public Func<Endpoint, bool> Match { get; } = match;
        public int Occurrence { get; } = occurrence;
        public Func<Task> Action { get; } = action;
        public int Seen;
    }

    private sealed record Fault(Func<Endpoint, bool> Match, int? StatusCode, string? Body, Exception? Exception, Func<Task>? AfterFault);

    public ResponseFactory ResponseFactory => inner.ResponseFactory;

    /// <summary>Number of requests answered with an injected fault.</summary>
    public int InjectedCount => _injected;

    /// <summary>Ids of bulk items whose reported outcome was rewritten to a failure.</summary>
    public IReadOnlyCollection<string> FailedBulkItemIds => _failedBulkItemIds.ToArray();

    /// <summary>
    /// Fails the next <paramref name="count"/> requests whose method and path match with <paramref name="statusCode"/>.
    /// <paramref name="afterFault"/> runs after each injected failure and before it is returned, for example to change
    /// the document or dispose the repository between retry rounds.
    /// </summary>
    public void FailNext(int count, int statusCode, HttpMethod method, string pathContains, Func<Task>? afterFault = null)
    {
        string body = $$"""{"error":{"type":"injected_fault","reason":"injected {{statusCode}}"},"status":{{statusCode}}}""";
        RespondNext(count, statusCode, body, method, pathContains, afterFault);
    }

    /// <summary>Answers the next <paramref name="count"/> matching requests with <paramref name="body"/> without sending them.</summary>
    public void RespondNext(int count, int statusCode, string body, HttpMethod method, string pathContains, Func<Task>? afterFault = null)
    {
        for (int i = 0; i < count; i++)
            _faults.Enqueue(new Fault(Matches(method, pathContains), statusCode, body, null, afterFault));
    }

    /// <summary>
    /// Fails the next <paramref name="count"/> matching requests as if the transport threw <paramref name="exception"/>
    /// (for example a refused connection or a timeout), without contacting Elasticsearch.
    /// </summary>
    public void ThrowNext(int count, Exception exception, HttpMethod method, string pathContains)
    {
        for (int i = 0; i < count; i++)
            _faults.Enqueue(new Fault(Matches(method, pathContains), null, null, exception, null));
    }

    /// <summary>
    /// Rewrites the response of the next <c>_bulk</c> request so its first item reports <paramref name="status"/> with the
    /// given error. The real write still happened in Elasticsearch; only the reported outcome changes. The id of the
    /// rewritten item is added to <see cref="FailedBulkItemIds"/>.
    /// </summary>
    public void FailFirstItemOfNextBulk(int status, string type, string reason) => _nextBulkItemFailure = (status, type, reason);

    /// <summary>Rewrites the first document of the next <c>_mget</c> response into an item-level error.</summary>
    public void FailFirstDocOfNextMultiGet(string type, string reason) => _nextMultiGetItemFailure = (type, reason);

    /// <summary>
    /// Runs <paramref name="action"/> once, right before the next <c>_bulk</c> request is sent. Use it to change documents
    /// after they were read and before they are written, which makes version conflicts deterministic.
    /// </summary>
    public void BeforeNextBulk(Func<Task> action) => _beforeNextBulk = action;

    /// <summary>
    /// Runs <paramref name="action"/> once, right before the <paramref name="occurrence"/>th (1-based) matching request from
    /// now on is sent. Use it to dispose the repository or change documents in the middle of a sequence of requests.
    /// </summary>
    public void BeforeNthRequest(int occurrence, HttpMethod method, string pathContains, Func<Task> action)
    {
        _beforeRequests.Add(new BeforeRequest(Matches(method, pathContains), occurrence, action));
    }

    /// <summary>Counts the requests sent (including injected faults) whose method and path match.</summary>
    public int CountRequests(HttpMethod method, string pathContains)
    {
        return _requests.Count(r => r.Method == method && r.Path.Contains(pathContains, StringComparison.Ordinal));
    }

    private static Func<Endpoint, bool> Matches(HttpMethod method, string pathContains)
    {
        return endpoint => endpoint.Method == method && endpoint.PathAndQuery.Contains(pathContains, StringComparison.Ordinal);
    }

    public async Task<TResponse> RequestAsync<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData, CancellationToken cancellationToken)
        where TResponse : TransportResponse, new()
    {
        _requests.Enqueue((endpoint.Method, endpoint.PathAndQuery));
        if (endpoint.PathAndQuery.Contains("_bulk", StringComparison.Ordinal) && Interlocked.Exchange(ref _beforeNextBulk, null) is { } beforeBulk)
            await beforeBulk().ConfigureAwait(false);

        foreach (var before in _beforeRequests.ToArray())
        {
            if (before.Match(endpoint) && Interlocked.Increment(ref before.Seen) == before.Occurrence)
                await before.Action().ConfigureAwait(false);
        }

        if (TryTakeFault(endpoint, out var fault))
        {
            if (fault.AfterFault is not null)
                await fault.AfterFault().ConfigureAwait(false);

            return CreateFaultResponse<TResponse>(endpoint, boundConfiguration, postData, fault);
        }

        var response = await inner.RequestAsync<TResponse>(endpoint, boundConfiguration, postData, cancellationToken).ConfigureAwait(false);
        if (endpoint.PathAndQuery.Contains("_mget", StringComparison.Ordinal) && _nextMultiGetItemFailure is { } multiGetFailure)
        {
            _nextMultiGetItemFailure = null;
            return Rewrite(response, endpoint, boundConfiguration, postData, root =>
            {
                var docs = root["docs"]!.AsArray();
                var doc = docs[0]!.AsObject();
                docs[0] = new JsonObject
                {
                    ["_index"] = doc["_index"]!.GetValue<string>(),
                    ["_id"] = doc["_id"]!.GetValue<string>(),
                    ["error"] = new JsonObject { ["type"] = multiGetFailure.Type, ["reason"] = multiGetFailure.Reason }
                };
            });
        }

        if (endpoint.PathAndQuery.Contains("_bulk", StringComparison.Ordinal) && _nextBulkItemFailure is { } failure)
        {
            _nextBulkItemFailure = null;
            return Rewrite(response, endpoint, boundConfiguration, postData, root =>
            {
                var item = root["items"]!.AsArray()[0]!.AsObject().First().Value!.AsObject();
                _failedBulkItemIds.Enqueue(item["_id"]!.GetValue<string>());
                item.Remove("result");
                item.Remove("_seq_no");
                item.Remove("_primary_term");
                item["status"] = failure.Status;
                item["error"] = new JsonObject { ["type"] = failure.Type, ["reason"] = failure.Reason };
                root["errors"] = true;
            });
        }

        return response;
    }

    private TResponse Rewrite<TResponse>(TResponse response, Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData, Action<JsonObject> rewrite)
        where TResponse : TransportResponse, new()
    {
        var bytes = response.ApiCallDetails.ResponseBodyInBytes
            ?? throw new InvalidOperationException("Rewriting responses requires DisableDirectStreaming.");

        var root = JsonNode.Parse(bytes)!.AsObject();
        rewrite(root);
        return CreateFaultResponse<TResponse>(endpoint, boundConfiguration, postData, new Fault(_ => true, 200, root.ToJsonString(), null, null));
    }

    public TResponse Request<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData)
        where TResponse : TransportResponse, new()
    {
        if (TryTakeFault(endpoint, out var fault))
            return CreateFaultResponse<TResponse>(endpoint, boundConfiguration, postData, fault);

        return inner.Request<TResponse>(endpoint, boundConfiguration, postData);
    }

    private bool TryTakeFault(Endpoint endpoint, out Fault fault)
    {
        if (_faults.TryPeek(out fault!) && fault.Match(endpoint) && _faults.TryDequeue(out fault!))
        {
            Interlocked.Increment(ref _injected);
            return true;
        }

        return false;
    }

    private TResponse CreateFaultResponse<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData, Fault fault)
        where TResponse : TransportResponse, new()
    {
        if (fault.Exception is not null)
            return ResponseFactory.Create<TResponse>(endpoint, boundConfiguration, postData, fault.Exception, null, null, System.IO.Stream.Null, null, -1, null, null);

        string body = fault.Body ?? String.Empty;
        using var stream = new System.IO.MemoryStream(Encoding.UTF8.GetBytes(body));
        return ResponseFactory.Create<TResponse>(endpoint, boundConfiguration, postData, null, fault.StatusCode, null, stream, "application/json", body.Length, null, null);
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) is 0)
            inner.Dispose();
    }
}
