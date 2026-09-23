using System;
using System.Linq;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Exceptions;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public partial class IndexCompatibilityTests
{
    [Theory]
    [InlineData(502)]
    [InlineData(503)]
    [InlineData(504)]
    public async Task RunCompatibilityReindexAsync_WithRetryingTransport_SubmitsOnlyOnce(int statusCode)
    {
        // Arrange: all nodes could accept the first request even though its response is unavailable.
        var responses = Enumerable.Range(0, 3)
            .Select(_ => new StubResponse(statusCode, """{"error":{"type":"unavailable_shards_exception","reason":"submission outcome unknown"}}""", Request: "POST /_reindex"))
            .ToArray();
        using var invoker = new SequenceRequestInvoker(responses);
        using var nodes = new StaticNodePool([
            new Uri("http://node-one:9200"),
            new Uri("http://node-two:9200"),
            new Uri("http://node-three:9200")]);
        var settings = new ElasticsearchClientSettings(nodes, invoker).MaximumRetries(2).DisablePing();
        var runner = new ElasticReindexTaskRunner(new ElasticsearchClient(settings), TimeProvider.System);
        bool terminated = false;

        // Act
        await Assert.ThrowsAsync<ElasticReindexTaskUncertainException>(() => runner.RunCompatibilityReindexAsync(
            "employees", "reindexed-v9-employees", null, null, (_, _) => Task.CompletedTask,
            () => terminated = true, TestContext.Current.CancellationToken));

        // Assert: neither a second submission nor cleanup authority is permitted after ambiguous dispatch.
        Assert.Equal(["POST /_reindex"], invoker.Requests);
        Assert.Equal(2, invoker.RemainingResponses);
        Assert.False(terminated);
    }

    [Fact]
    public void SingleAttemptRequest_RespectsNodePredicateWithoutChangingGlobalRetries()
    {
        using var invoker = new SequenceRequestInvoker([]);
        using var nodes = new StaticNodePool([
            new Uri("http://excluded-one:9200"), new Uri("http://allowed:9200"), new Uri("http://excluded-two:9200")]);
        var settings = new ElasticsearchClientSettings(nodes, invoker)
            .NodePredicate(node => node.Uri.Host == "allowed")
            .MaximumRetries(2);
        var client = new ElasticsearchClient(settings);

        for (int i = 0; i < 3; i++)
        {
            IRequestConfiguration request = SingleAttemptRequest.Configure(client, new RequestConfigurationDescriptor());
            Assert.Equal("allowed", request.ForceNode?.Host);
            Assert.Equal(0, new BoundConfiguration(settings, request).MaxRetries);
        }

        Assert.Equal(2, new BoundConfiguration(settings).MaxRetries);
        Assert.Empty(invoker.Requests);
    }

    [Fact]
    public void SingleAttemptRequest_WhenEveryNodeIsExcluded_RefusesBeforeDispatch()
    {
        using var invoker = new SequenceRequestInvoker([]);
        using var nodes = new StaticNodePool([new Uri("http://excluded:9200")]);
        var settings = new ElasticsearchClientSettings(nodes, invoker).NodePredicate(_ => false);
        var client = new ElasticsearchClient(settings);

        var exception = Assert.Throws<RepositoryException>(() =>
            SingleAttemptRequest.Configure(client, new RequestConfigurationDescriptor()));

        Assert.Contains("No eligible", exception.Message);
        Assert.Empty(invoker.Requests);
    }
}
