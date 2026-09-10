using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Foundatio.Repositories.Elasticsearch.Tests.Infrastructure;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

/// <summary>
/// Verifies the disposable-cluster guard refuses unverified targets and issues no destructive request
/// while doing so.
/// </summary>
/// <remarks>
/// Runs entirely against an in-memory transport. It never contacts a real cluster - demonstrating the
/// guard by pointing it at an unrelated cluster would risk the exact data loss it prevents.
/// </remarks>
public sealed class DisposableClusterGuardTests
{
    /// <summary>Methods and endpoints that can destroy or overwrite data.</summary>
    private static readonly Regex _destructiveEndpoint = new(
        @"(_delete_by_query|_update_by_query|/_bulk|_forcemerge|_close|_aliases|_reindex|/_settings)",
        RegexOptions.Compiled | RegexOptions.IgnoreCase);

    /// <summary>
    /// Headers the client's product check requires; without them every response is rejected as
    /// "not a supported distribution of Elasticsearch" before the guard's own logic is reached.
    /// </summary>
    private static Dictionary<string, IEnumerable<string>> ProductHeaders() =>
        new() { ["x-elastic-product"] = ["Elasticsearch"] };

    private static (ElasticsearchClient Client, ConcurrentQueue<string> Requests) CreateObservedClient(
        int statusCode, string responseBody, string? optIn = DisposableClusterGuard.OptInValue)
    {
        Environment.SetEnvironmentVariable(DisposableClusterGuard.OptInVariable, optIn);

        var requests = new ConcurrentQueue<string>();
        var pool = new SingleNodePool(new Uri("http://in-memory.invalid:9200"));
        var settings = new ElasticsearchClientSettings(pool, new InMemoryRequestInvoker(
                responseBody: System.Text.Encoding.UTF8.GetBytes(responseBody),
                statusCode: statusCode,
                exception: null,
                contentType: "application/json",
                headers: ProductHeaders()))
            .DisableDirectStreaming()
            .OnRequestCompleted(details => requests.Enqueue($"{details.HttpMethod} {details.Uri?.AbsolutePath ?? "(no uri)"}"));

        return (new ElasticsearchClient(settings), requests);
    }

    private static void AssertNoDestructiveRequests(IEnumerable<string> requests)
    {
        var observed = requests.ToList();

        var deletes = observed.Where(r => r.StartsWith("DELETE", StringComparison.OrdinalIgnoreCase)).ToList();
        Assert.True(deletes.Count is 0, $"Guard issued DELETE request(s): {String.Join(" | ", deletes)}");

        var destructive = observed.Where(r => _destructiveEndpoint.IsMatch(r)).ToList();
        Assert.True(destructive.Count is 0, $"Guard issued destructive request(s): {String.Join(" | ", destructive)}");
    }

    [Fact]
    public async Task ValidateAsync_WhenMarkerBelongsToSomethingElse_RefusesAndIssuesNoDestructiveRequest()
    {
        // Arrange: a cluster that is marked, but as someone else's.
        var (client, requests) = CreateObservedClient(200,
            """{"_index":"foundatio-disposable-test-cluster","_id":"marker","found":true,"_source":{"purpose":"someone-elses-production-cluster"}}""");

        // Act
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => DisposableClusterGuard.ValidateAsync(client, TestContext.Current.CancellationToken));

        // Assert
        Assert.Contains("belongs to something else", ex.Message);
        Assert.Contains("someone-elses-production-cluster", ex.Message);
        AssertNoDestructiveRequests(requests);
    }

    [Fact]
    public async Task ValidateAsync_WhenMarkerAbsentAndClusterHasUserIndexes_RefusesAndIssuesNoDestructiveRequest()
    {
        // Arrange: unmarked cluster that already holds data, i.e. possibly a real environment.
        // The marker GET 404s, then the emptiness check returns existing indexes.
        var requests = new ConcurrentQueue<string>();
        Environment.SetEnvironmentVariable(DisposableClusterGuard.OptInVariable, DisposableClusterGuard.OptInValue);

        var pool = new SingleNodePool(new Uri("http://in-memory.invalid:9200"));
        var settings = new ElasticsearchClientSettings(pool, new SequencedRequestInvoker(
            [
                (404, """{"_index":"foundatio-disposable-test-cluster","_id":"marker","found":false}"""),
                (200, """{"customers":{"aliases":{},"mappings":{},"settings":{}},"orders":{"aliases":{},"mappings":{},"settings":{}}}""")
            ]))
            .DisableDirectStreaming()
            .OnRequestCompleted(details => requests.Enqueue($"{details.HttpMethod} {details.Uri?.AbsolutePath ?? "(no uri)"}"));
        var client = new ElasticsearchClient(settings);

        // Act
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => DisposableClusterGuard.ValidateAsync(client, TestContext.Current.CancellationToken));

        // Assert
        Assert.Contains("may be a real environment", ex.Message);
        Assert.Contains("customers", ex.Message);
        AssertNoDestructiveRequests(requests);
    }

    [Fact]
    public async Task ValidateAsync_WhenMarkerUnreadable_FailsClosedAndIssuesNoDestructiveRequest()
    {
        // Arrange: the marker read fails outright, so the target cannot be verified either way.
        var (client, requests) = CreateObservedClient(503, """{"error":{"reason":"cluster unavailable"}}""");

        // Act
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => DisposableClusterGuard.ValidateAsync(client, TestContext.Current.CancellationToken));

        // Assert
        Assert.Contains("could not read the disposable-cluster marker", ex.Message);
        Assert.Contains("fails closed", ex.Message);
        AssertNoDestructiveRequests(requests);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("true")]
    [InlineData("yes")]
    public async Task ValidateAsync_WithoutExplicitOptIn_RefusesBeforeContactingTheCluster(string? optIn)
    {
        // Arrange
        var (client, requests) = CreateObservedClient(200,
            """{"_index":"foundatio-disposable-test-cluster","_id":"marker","found":true,"_source":{"purpose":"foundatio-repositories-integration-tests"}}""",
            optIn);

        // Act
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => DisposableClusterGuard.ValidateAsync(client, TestContext.Current.CancellationToken));

        // Assert: refused even though the cluster carries a valid marker, and refused without any request.
        Assert.Contains("not explicitly declared disposable", ex.Message);
        Assert.Empty(requests);
    }

    [Fact]
    public async Task ValidateAsync_WhenMarkerMatches_Succeeds()
    {
        // Arrange
        var (client, requests) = CreateObservedClient(200,
            """{"_index":"foundatio-disposable-test-cluster","_id":"marker","found":true,"_source":{"purpose":"foundatio-repositories-integration-tests"}}""");

        // Act
        await DisposableClusterGuard.ValidateAsync(client, TestContext.Current.CancellationToken);

        // Assert: a single read to confirm the marker, and nothing destructive.
        Assert.Single(requests);
        AssertNoDestructiveRequests(requests);
    }

    /// <summary>Returns a different canned response per call so multi-step guard paths can be exercised.</summary>
    private sealed class SequencedRequestInvoker : IRequestInvoker
    {
        private readonly InMemoryRequestInvoker[] _invokers;
        private int _index = -1;

        public SequencedRequestInvoker((int StatusCode, string Body)[] responses)
        {
            _invokers = responses
                .Select(r => new InMemoryRequestInvoker(
                    System.Text.Encoding.UTF8.GetBytes(r.Body),
                    r.StatusCode,
                    exception: null,
                    contentType: "application/json",
                    headers: ProductHeaders()))
                .ToArray();
        }

        public ResponseFactory ResponseFactory => Current.ResponseFactory;

        private InMemoryRequestInvoker Current => _invokers[Math.Clamp(_index < 0 ? 0 : _index, 0, _invokers.Length - 1)];

        private InMemoryRequestInvoker Next() => _invokers[Math.Min(Interlocked.Increment(ref _index), _invokers.Length - 1)];

        public TResponse Request<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData)
            where TResponse : TransportResponse, new()
            => Next().Request<TResponse>(endpoint, boundConfiguration, postData);

        public Task<TResponse> RequestAsync<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData, CancellationToken cancellationToken = default)
            where TResponse : TransportResponse, new()
            => Next().RequestAsync<TResponse>(endpoint, boundConfiguration, postData, cancellationToken);

        public void Dispose() { }
    }
}
