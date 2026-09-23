using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Foundatio.Repositories.Elasticsearch.Jobs;
using Foundatio.Repositories.Elasticsearch.Tests.Infrastructure;
using Foundatio.Repositories.Exceptions;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class ReindexDispatchSafetyTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Sequential_Task404NeverClearsTheDurableDispatchFence(bool afterCancellation)
    {
        var work = new ReindexWorkItem { Alias = "alias", OldIndex = "source", NewIndex = "destination" };
        var entry = new ReindexSafetyState.Entry("task", "source", "destination", "alias", String.Empty, TaskId: "node:1");
        var responses = new List<(int, string)> { (200, JsonSerializer.Serialize(new { _source = entry })) };
        if (afterCancellation)
        {
            responses.Add((200, """{"completed":false,"task":{"node":"node","id":1,"type":"transport","action":"indices:data/write/reindex","start_time_in_millis":1,"running_time_in_nanos":1,"cancellable":true,"headers":{}}}"""));
            responses.Add((200, """{"nodes":{}}"""));
        }
        responses.Add((404, """{"error":{"type":"resource_not_found_exception","reason":"owner unavailable and no stored result"},"status":404}"""));
        using var invoker = new SequenceRequestInvoker([.. responses]);
        using var nodes = new SingleNodePool(new Uri("http://in-memory.invalid:9200"));
        var client = new ElasticsearchClient(new ElasticsearchClientSettings(nodes, invoker).MaximumRetries(0));

        await Assert.ThrowsAsync<ReindexCompletionUnknownException>(() =>
            ReindexTaskLease.AcquireAsync(client, work, NullLogger.Instance, TestContext.Current.CancellationToken));

        Assert.Equal(afterCancellation ? 4 : 2, invoker.Requests.Count);
        Assert.DoesNotContain(invoker.Requests, request => request.StartsWith("DELETE", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData(502)]
    [InlineData(503)]
    [InlineData(504)]
    public async Task Sequential_SchemaLaunchDoesNotRetryAnAmbiguousSubmission(int statusCode)
    {
        const string refresh = """{"_shards":{"total":1,"successful":1,"failed":0}}""";
        var responses = new List<(int, string)>
        {
            (200, refresh), (200, refresh), (404, "{}"),
            (200, """{"acknowledged":true}"""), (201, """{"result":"created"}""")
        };
        responses.AddRange(Enumerable.Repeat((statusCode, """{"error":{"type":"unavailable_shards_exception","reason":"unknown launch"}}"""), 3));
        using var invoker = new SequenceRequestInvoker([.. responses]);
        using var nodes = new StaticNodePool([
            new Uri("http://node-one:9200"), new Uri("http://node-two:9200"), new Uri("http://node-three:9200")]);
        var client = new ElasticsearchClient(new ElasticsearchClientSettings(nodes, invoker).MaximumRetries(2).DisablePing());
        var reindexer = new ElasticReindexer(client, new Foundatio.Serializer.SystemTextJsonSerializer());
        var work = new ReindexWorkItem { Alias = "alias", OldIndex = "source", NewIndex = "destination" };
        var method = typeof(ElasticReindexer).GetMethod("InternalReindexAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        Func<int, string?, Task> progress = (_, _) => Task.CompletedTask;

        // Isolate one copy pass so recovery/preflight reads cannot hide a retried launch.
        await (Task)method.Invoke(reindexer, [work, progress, 0, 90, null, TestContext.Current.CancellationToken])!;

        Assert.Single(invoker.Requests.Where(request => request == "POST /_reindex"));
        Assert.DoesNotContain(invoker.Requests, request => request.StartsWith("DELETE", StringComparison.Ordinal));
    }
}
