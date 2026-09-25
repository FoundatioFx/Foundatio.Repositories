using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Foundatio.Repositories.Elasticsearch.Jobs;
using Foundatio.Repositories.Elasticsearch.Tests.Infrastructure;
using Foundatio.Repositories.Exceptions;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class ReindexWorkItemHandlerTests
{
    [Theory]
    [InlineData("employees-v2")]
    [InlineData("employees-v2-2026.09.11")]
    [InlineData("employees-v2-2026.09")]
    public async Task IsAlreadyPromotedAsync_ChecksTheWorkItemsExactDestination(string destination)
    {
        // No registered index or deployed schema version is needed. Other partitions may still be on v1.
        var body = JsonSerializer.Serialize(new Dictionary<string, object>
        {
            [destination] = new { aliases = new Dictionary<string, object> { ["employees"] = new { } } }
        });
        var (client, requests) = CreateClient(200, body);
        var workItem = new ReindexWorkItem { Alias = "employees", OldIndex = "employees-v1", NewIndex = destination };

        Assert.True(await ReindexWorkItemHandler.IsAlreadyPromotedAsync(client, workItem, TestContext.Current.CancellationToken));

        Assert.Equal([$"GET /{destination}/_alias"], requests);
    }

    [Theory]
    [InlineData("{\"employees-v2\":{\"aliases\":{}}}")]
    [InlineData("{\"employees-v2\":{\"aliases\":{\"other\":{}}}}")]
    [InlineData("{\"employees-v2\":{\"aliases\":{\"Employees\":{}}}}")]
    public async Task IsAlreadyPromotedAsync_RequiresTheExactAlias(string body)
    {
        var (client, requests) = CreateClient(200, body);

        Assert.False(await ReindexWorkItemHandler.IsAlreadyPromotedAsync(client, CreateWorkItem(), TestContext.Current.CancellationToken));

        Assert.Single(requests);
    }

    [Fact]
    public async Task IsAlreadyPromotedAsync_WhenDestinationDoesNotExist_ReturnsFalse()
    {
        var (client, requests) = CreateClient(404, """
            {"error":{"type":"index_not_found_exception","reason":"no such index [employees-v2]"},"status":404}
            """);

        Assert.False(await ReindexWorkItemHandler.IsAlreadyPromotedAsync(client, CreateWorkItem(), TestContext.Current.CancellationToken));

        Assert.Single(requests);
    }

    [Theory]
    [InlineData(403)]
    [InlineData(429)]
    [InlineData(503)]
    public async Task IsAlreadyPromotedAsync_WhenAliasReadFails_DoesNotTreatDestinationAsUnpromoted(int statusCode)
    {
        var (client, requests) = CreateClient(statusCode, """
            {"error":{"type":"unavailable","reason":"simulated alias read failure"}}
            """);

        await Assert.ThrowsAsync<RepositoryException>(() => ReindexWorkItemHandler.IsAlreadyPromotedAsync(client, CreateWorkItem(), TestContext.Current.CancellationToken));

        Assert.Equal(["GET /employees-v2/_alias"], requests);
    }

    [Theory]
    [InlineData("{}")]
    [InlineData("{\"employees-v1\":{\"aliases\":{\"employees\":{}}}}")]
    [InlineData("{\"employees-v2\":{\"aliases\":null}}")]
    [InlineData("{\"employees-v2\":{\"aliases\":{}},\"employees-v3\":{\"aliases\":{}}}")]
    public async Task IsAlreadyPromotedAsync_WhenResponseDoesNotIdentifyOneDestination_FailsClosed(string body)
    {
        var (client, requests) = CreateClient(200, body);

        await Assert.ThrowsAsync<RepositoryException>(() => ReindexWorkItemHandler.IsAlreadyPromotedAsync(client, CreateWorkItem(), TestContext.Current.CancellationToken));

        Assert.Single(requests);
    }

    [Fact]
    public async Task IsAlreadyPromotedAsync_WhenCancelled_DoesNotReadAliasState()
    {
        var (client, requests) = CreateClient(200, "{}");
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => ReindexWorkItemHandler.IsAlreadyPromotedAsync(client, CreateWorkItem(), cancellation.Token));

        Assert.Empty(requests);
    }

    private static ReindexWorkItem CreateWorkItem() => new()
    {
        Alias = "employees",
        OldIndex = "employees-v1",
        NewIndex = "employees-v2"
    };

    private static (ElasticsearchClient Client, ConcurrentQueue<string> Requests) CreateClient(int statusCode, string body)
    {
        var requests = new ConcurrentQueue<string>();
        var settings = new ElasticsearchClientSettings(
                new SingleNodePool(new Uri("http://in-memory.invalid:9200")),
                new SequenceRequestInvoker((statusCode, body)))
            .MaximumRetries(0)
            .OnRequestCompleted(details => requests.Enqueue($"{details.HttpMethod} {details.Uri?.AbsolutePath}"));

        return (new ElasticsearchClient(settings), requests);
    }
}
