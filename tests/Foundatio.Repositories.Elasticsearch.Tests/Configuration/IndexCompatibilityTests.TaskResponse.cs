using System;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Foundatio.Repositories.Elasticsearch.Jobs;
using Foundatio.Repositories.Exceptions;
using Foundatio.Resilience;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public partial class IndexCompatibilityTests
{
    private const string EmptyTaskResponse = """{"total":0,"created":0,"updated":0,"deleted":0,"noops":0,"version_conflicts":0,"failures":[]}""";
    private static readonly string[] TaskCounterNames = ["total", "created", "updated", "deleted", "noops", "version_conflicts"];

    [Theory]
    [InlineData("null")]
    [InlineData("true")]
    [InlineData("false")]
    [InlineData("\"1\"")]
    [InlineData("1.5")]
    [InlineData("-1")]
    [InlineData("9223372036854775808")]
    [InlineData("{}")]
    public void TaskResponse_RejectsMalformedJsonCounters(string value)
    {
        foreach (string counter in TaskCounterNames)
        {
            // Arrange
            using var document = JsonDocument.Parse(EmptyTaskResponse.Replace($"\"{counter}\":0", $"\"{counter}\":{value}", StringComparison.Ordinal));
            var dictionary = document.RootElement.EnumerateObject().ToDictionary(property => property.Name, property => (object)property.Value);

            // Act & Assert
            Assert.Null(ElasticReindexTaskResponseReader.ReadStatus(document.RootElement));
            Assert.Null(ElasticReindexTaskResponseReader.ReadCompleted(document.RootElement));
            Assert.Null(ElasticReindexTaskResponseReader.ReadStatus(dictionary));
            Assert.Null(ElasticReindexTaskResponseReader.ReadCompleted(dictionary));
        }
    }

    [Theory]
    [InlineData("boolean")]
    [InlineData("string")]
    [InlineData("double")]
    [InlineData("single")]
    [InlineData("decimal")]
    [InlineData("negative")]
    [InlineData("overflow")]
    [InlineData("null")]
    public void TaskResponse_DoesNotCoerceDictionaryCounters(string kind)
    {
        foreach (string counter in TaskCounterNames)
        {
            // Arrange
            var values = TaskCounterNames.ToDictionary(name => name, _ => (object)0L);
            values["failures"] = Array.Empty<object>();
            values[counter] = kind switch
            {
                "boolean" => true,
                "string" => "1",
                "double" => 1.5d,
                "single" => 1.5f,
                "decimal" => 1.5m,
                "negative" => -1L,
                "overflow" => UInt64.MaxValue,
                "null" => null!,
                _ => throw new ArgumentOutOfRangeException(nameof(kind))
            };

            // Act & Assert
            Assert.Null(ElasticReindexTaskResponseReader.ReadStatus(values));
            Assert.Null(ElasticReindexTaskResponseReader.ReadCompleted(values));
        }
    }

    [Theory]
    [InlineData("byte")]
    [InlineData("sbyte")]
    [InlineData("short")]
    [InlineData("ushort")]
    [InlineData("int")]
    [InlineData("uint")]
    [InlineData("long")]
    [InlineData("ulong")]
    public void TaskResponse_PreservesSupportedIntegerCounters(string kind)
    {
        // Arrange
        object one = kind switch
        {
            "byte" => (object)(byte)1,
            "sbyte" => (object)(sbyte)1,
            "short" => (object)(short)1,
            "ushort" => (object)(ushort)1,
            "int" => (object)1,
            "uint" => (object)1U,
            "long" => (object)1L,
            "ulong" => (object)1UL,
            _ => throw new ArgumentOutOfRangeException(nameof(kind))
        };
        var values = TaskCounterNames.ToDictionary(name => name, _ => (object)0L);
        values["total"] = one;
        values["created"] = one;
        values["failures"] = Array.Empty<object>();

        // Act
        var status = ElasticReindexTaskResponseReader.ReadStatus(values);
        var response = ElasticReindexTaskResponseReader.ReadCompleted(values);

        // Assert
        Assert.NotNull(status);
        Assert.NotNull(response);
        Assert.Equal(1L, status.Value.Total);
        Assert.Equal(1L, response.Total);
        Assert.Equal(1L, response.Created);
    }

    [Theory]
    [InlineData("null")]
    [InlineData("\"true\"")]
    [InlineData("1")]
    [InlineData("{}")]
    [InlineData("[]")]
    public void TaskResponse_RejectsMalformedTimeoutFlags(string value)
    {
        // Arrange
        using var document = JsonDocument.Parse(EmptyTaskResponse.Replace("\"failures\":[]", $"\"timed_out\":{value},\"failures\":[]", StringComparison.Ordinal));
        var dictionary = document.RootElement.EnumerateObject().ToDictionary(property => property.Name, property => (object)property.Value);

        // Act & Assert
        Assert.Null(ElasticReindexTaskResponseReader.ReadCompleted(document.RootElement));
        Assert.Null(ElasticReindexTaskResponseReader.ReadCompleted(dictionary));
    }

    [Fact]
    public void TaskResponse_WireCountersDoNotUseApplicationNamingPolicy()
    {
        // Arrange
        using var document = JsonDocument.Parse("""{"total":3,"created":2,"updated":0,"deleted":0,"noops":0,"version_conflicts":1,"failures":[]}""");
        var serializer = new Foundatio.Serializer.SystemTextJsonSerializer(new JsonSerializerOptions { PropertyNameCaseInsensitive = false });

        // Act
        var response = ElasticReindexTaskResponseReader.ReadCompleted(document.RootElement, serializer);

        // Assert
        Assert.NotNull(response);
        Assert.Equal(3L, response.Total);
        Assert.Equal(2L, response.Created);
        Assert.Equal(1L, response.VersionConflicts);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task TaskResponse_CompatibilityRunnerChecksTimeoutWithOrWithoutBufferedBody(bool timedOut, bool bufferResponse)
    {
        // Arrange
        var invoker = new SequenceRequestInvoker(
            new StubResponse(200, """{"task":"node:1"}""", Request: "POST /_reindex"),
            new StubResponse(200, CreateCompletedTaskResponse(timedOut), Request: "GET /_tasks/node:1"));
        var client = new ElasticsearchClient(new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://localhost:9200")), invoker)
            .DisableDirectStreaming(bufferResponse));
        var runner = new ElasticReindexTaskRunner(client, TimeProvider.System);
        int terminated = 0;

        // Act & Assert
        if (timedOut)
        {
            var exception = await Assert.ThrowsAsync<RepositoryException>(() => runner.RunCompatibilityReindexAsync(
                "employees-v1", "reindexed-v9-employees-v1", null, null, (_, _) => Task.CompletedTask, () => terminated++, TestContext.Current.CancellationToken));
            Assert.Contains("Timed out: True", exception.Message, StringComparison.Ordinal);
        }
        else
        {
            var response = await runner.RunCompatibilityReindexAsync(
                "employees-v1", "reindexed-v9-employees-v1", null, null, (_, _) => Task.CompletedTask, () => terminated++, TestContext.Current.CancellationToken);
            Assert.Equal(1L, response.Total);
            Assert.Equal(1L, response.Created);
        }
        Assert.Equal(1, terminated);
        Assert.Equal(0, invoker.RemainingResponses);
        Assert.Equal(2, invoker.Requests.Count);
    }

    [Fact]
    public async Task TaskResponse_SchemaReindexDoesNotPromoteTimedOutCopy()
    {
        // Arrange
        string completed = CreateCompletedTaskResponse(true);
        var invoker = new SequenceRequestInvoker(
            new StubResponse(200, """{"employees-v1":{"aliases":{"employees":{}}}}""", Request: "GET /employees-v1"),
            new StubResponse(200, """{"employees-v1":{"aliases":{"employees":{}}}}""", Request: "GET /employees-v1/_alias"),
            new StubResponse(200, """{"took":0,"timed_out":false,"_shards":{"total":1,"successful":1,"failed":0},"hits":{"total":{"value":0,"relation":"eq"},"hits":[]}}"""),
            new StubResponse(200, """{"task":"node:1"}""", Request: "POST /_reindex"),
            new StubResponse(200, completed, Request: "GET /_tasks/node:1"),
            new StubResponse(200, """{"nodes":{}}""", Request: "POST /_tasks/node:1/_cancel"),
            new StubResponse(200, completed, Request: "GET /_tasks/node:1"));
        var client = new ElasticsearchClient(new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://localhost:9200")), invoker));
        var reindexer = new ElasticReindexer(client, new Foundatio.Serializer.SystemTextJsonSerializer(), TimeProvider.System, resiliencePolicyProvider: new ResiliencePolicyProvider());
        var workItem = new ReindexWorkItem { OldIndex = "employees-v1", NewIndex = "employees-v2", Alias = "employees", TimestampField = "updatedUtc", DeleteOld = true };

        // Act
        await reindexer.ReindexAsync(workItem);

        // Assert
        Assert.DoesNotContain(invoker.Requests, request => request.Contains("/_aliases", StringComparison.Ordinal) || request.StartsWith("DELETE ", StringComparison.Ordinal));
        Assert.Equal(0, invoker.RemainingResponses);
    }

    private static string CreateCompletedTaskResponse(bool timedOut)
    {
        return """
            {
              "completed":true,
              "task":{"node":"node","id":1,"type":"transport","action":"indices:data/write/reindex",
                "status":{"total":1,"created":1,"updated":0,"deleted":0,"noops":0,"version_conflicts":0},
                "start_time_in_millis":1,"running_time_in_nanos":1,"cancellable":true,"cancelled":false,"headers":{}},
              "response":{"total":1,"created":1,"updated":0,"deleted":0,"noops":0,"version_conflicts":0,
                "timed_out":TIMED_OUT,"failures":[]}
            }
            """.Replace("TIMED_OUT", timedOut ? "true" : "false", StringComparison.Ordinal);
    }
}
