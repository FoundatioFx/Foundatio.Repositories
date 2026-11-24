using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Foundatio.Repositories.Elasticsearch.Tests.Infrastructure;
using Foundatio.Repositories.Exceptions;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class IndexWriteBlockResponseTests
{
    private const string Unblocked = """{"source":{"settings":{"index":{}}}}""";
    private const string Blocked = """{"source":{"settings":{"index":{"blocks":{"write":"true"}}}}}""";
    private const string BlockConfirmed = """{"acknowledged":true,"shards_acknowledged":true,"indices":[{"name":"source","blocked":true}]}""";
    private const string Acknowledged = """{"acknowledged":true}""";

    private static (ElasticsearchClient Client, ConcurrentQueue<string> Requests) CreateClient(
        (int StatusCode, string Body)[] responses, Action<string>? requestCompleted = null)
    {
        var requests = new ConcurrentQueue<string>();
        var settings = new ElasticsearchClientSettings(
            new SingleNodePool(new Uri("http://in-memory.invalid:9200")), new SequenceRequestInvoker(responses))
            .MaximumRetries(0)
            .OnRequestCompleted(details =>
            {
                string path = details.Uri?.AbsolutePath ?? "(no uri)";
                requests.Enqueue($"{details.HttpMethod} {path}");
                requestCompleted?.Invoke(path);
            });

        return (new ElasticsearchClient(settings), requests);
    }

    [Theory]
    [InlineData(503, "{\"error\":{\"reason\":\"unavailable\"}}")]
    [InlineData(200, "")]
    [InlineData(200, "{")]
    [InlineData(200, "[]")]
    [InlineData(200, "{}")]
    [InlineData(200, "{\"other\":{\"settings\":{\"index\":{}}}}")]
    [InlineData(200, "{\"source\":{\"settings\":{}}}")]
    [InlineData(200, "{\"source\":{\"settings\":{\"index\":{\"blocks\":null}}}}")]
    [InlineData(200, "{\"source\":{\"settings\":{\"index\":{\"blocks\":{\"write\":\"unknown\"}}}}}")]
    public async Task ApplyAsync_WhenPriorStateIsUnknown_DoesNotMutate(int statusCode, string body)
    {
        var (client, requests) = CreateClient([(statusCode, body), (200, BlockConfirmed), (200, Acknowledged)]);

        await Assert.ThrowsAsync<RepositoryException>(() => IndexWriteBlock.ApplyAsync(
            client, "source", NullLogger.Instance, TestContext.Current.CancellationToken));

        Assert.Equal(["GET /source/_settings"], requests);
    }

    [Theory]
    [InlineData("")]
    [InlineData("{")]
    [InlineData("[]")]
    [InlineData("{\"acknowledged\":false,\"shards_acknowledged\":true,\"indices\":[{\"name\":\"source\",\"blocked\":true}]}")]
    [InlineData("{\"acknowledged\":true,\"shards_acknowledged\":false,\"indices\":[{\"name\":\"source\",\"blocked\":true}]}")]
    [InlineData("{\"acknowledged\":true,\"shards_acknowledged\":true,\"indices\":[{\"name\":\"source\",\"blocked\":false}]}")]
    [InlineData("{\"acknowledged\":true,\"shards_acknowledged\":true,\"indices\":[{\"name\":\"other\",\"blocked\":true}]}")]
    public async Task ApplyAsync_WhenConfirmationFails_RemovesNewBlock(string body)
    {
        var (client, requests) = CreateClient([(200, Unblocked), (200, body), (200, Acknowledged)]);

        await Assert.ThrowsAsync<RepositoryException>(() => IndexWriteBlock.ApplyAsync(
            client, "source", NullLogger.Instance, TestContext.Current.CancellationToken));

        Assert.Equal(["GET /source/_settings", "PUT /source/_block/write", "PUT /source/_settings"], requests);
    }

    [Fact]
    public async Task ApplyAsync_WhenBlockRequestFails_AttemptsCleanup()
    {
        var (client, requests) = CreateClient([(200, Unblocked), (503, "{}"), (200, Acknowledged)]);

        await Assert.ThrowsAsync<RepositoryException>(() => IndexWriteBlock.ApplyAsync(
            client, "source", NullLogger.Instance, TestContext.Current.CancellationToken));

        Assert.Equal(["GET /source/_settings", "PUT /source/_block/write", "PUT /source/_settings"], requests);
    }

    [Fact]
    public async Task ApplyAsync_WhenConfirmationFails_PreservesOperatorBlock()
    {
        var (client, requests) = CreateClient([(200, Blocked), (200, "{}"), (200, Acknowledged)]);

        await Assert.ThrowsAsync<RepositoryException>(() => IndexWriteBlock.ApplyAsync(
            client, "source", NullLogger.Instance, TestContext.Current.CancellationToken));

        Assert.Equal(["GET /source/_settings", "PUT /source/_block/write"], requests);
    }

    [Fact]
    public async Task ApplyAsync_WhenCallerCancelsDuringConfirmation_StillAttemptsCleanup()
    {
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        var (client, requests) = CreateClient([(200, Unblocked), (200, "{}"), (200, Acknowledged)], path =>
        {
            if (path.EndsWith("/_block/write", StringComparison.Ordinal))
                cancellation.Cancel();
        });

        var exception = await Record.ExceptionAsync(async () =>
        {
            await using var block = await IndexWriteBlock.ApplyAsync(client, "source", NullLogger.Instance, cancellation.Token);
        });

        Assert.NotNull(exception);
        Assert.Equal(["GET /source/_settings", "PUT /source/_block/write", "PUT /source/_settings"], requests);
    }

    [Theory]
    [InlineData("")]
    [InlineData("{")]
    [InlineData("[]")]
    [InlineData("{}")]
    [InlineData("{\"acknowledged\":false}")]
    public async Task ReleaseAsync_WhenNotAcknowledged_Throws(string body)
    {
        var (client, requests) = CreateClient([(200, Unblocked), (200, BlockConfirmed), (200, body)]);
        await using var block = await IndexWriteBlock.ApplyAsync(
            client, "source", NullLogger.Instance, TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<RepositoryException>(() => block.ReleaseAsync());

        Assert.Equal(3, requests.Count);
    }
}
