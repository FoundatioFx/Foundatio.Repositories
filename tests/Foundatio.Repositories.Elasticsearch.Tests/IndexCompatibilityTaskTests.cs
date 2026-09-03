using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Exceptions;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public partial class IndexCompatibilityTests
{
    [Fact]
    public void ReadCompleted_WithIncompletePayload_ReturnsNull()
    {
        using var document = JsonDocument.Parse("""{"total":0,"created":0}""");

        var result = ElasticReindexTaskResponseReader.ReadCompleted(document.RootElement);

        Assert.Null(result);
    }

    [Fact]
    public async Task RunCompatibilityReindexAsync_WhenStartTransportFails_ThrowsUncertainException()
    {
        // Arrange
        var transportException = new TimeoutException("The reindex response was not received.");
        var requestInvoker = new InMemoryRequestInvoker([], 500, transportException, "application/json");
        var client = new ElasticsearchClient(new ElasticsearchClientSettings(requestInvoker));
        var runner = new ElasticReindexTaskRunner(client, TimeProvider.System);

        // Act
        var exception = await Assert.ThrowsAsync<ElasticReindexTaskUncertainException>(() => runner.RunCompatibilityReindexAsync(
            "employees-v1", "reindexed-v9-employees-v1", null, null, (_, _) => Task.CompletedTask, CancellationToken.None));

        // Assert
        Assert.Contains("is unknown", exception.Message);
        Assert.Same(transportException, exception.InnerException);
    }

    [Fact]
    public async Task RunCompatibilityReindexAsync_WhenAcceptedWithoutTaskId_ThrowsUncertainException()
    {
        // Arrange
        var headers = new Dictionary<string, IEnumerable<string>> { ["x-elastic-product"] = ["Elasticsearch"] };
        var requestInvoker = new InMemoryRequestInvoker(Encoding.UTF8.GetBytes("{}"), 200, null, "application/json", headers);
        var client = new ElasticsearchClient(new ElasticsearchClientSettings(requestInvoker));
        var runner = new ElasticReindexTaskRunner(client, TimeProvider.System);

        // Act
        var exception = await Assert.ThrowsAsync<ElasticReindexTaskUncertainException>(() => runner.RunCompatibilityReindexAsync(
            "employees-v1", "reindexed-v9-employees-v1", null, null, (_, _) => Task.CompletedTask, CancellationToken.None));

        // Assert
        Assert.Contains("no task ID", exception.Message);
    }

    [Fact]
    public async Task RunCompatibilityReindexAsync_RequestDoesNotUseAutomaticSlicing()
    {
        var headers = new Dictionary<string, IEnumerable<string>> { ["x-elastic-product"] = ["Elasticsearch"] };
        var requestInvoker = new InMemoryRequestInvoker(Encoding.UTF8.GetBytes("{}"), 200, null, "application/json", headers);
        string? requestUri = null;
        var settings = new ElasticsearchClientSettings(requestInvoker)
            .OnRequestCompleted(call => requestUri = call.Uri?.PathAndQuery);
        var client = new ElasticsearchClient(settings);
        var runner = new ElasticReindexTaskRunner(client, TimeProvider.System);

        await Assert.ThrowsAsync<ElasticReindexTaskUncertainException>(() => runner.RunCompatibilityReindexAsync(
            "employees-v1", "reindexed-v9-employees-v1", null, null, (_, _) => Task.CompletedTask, CancellationToken.None));

        Assert.NotNull(requestUri);
        Assert.StartsWith("/_reindex", requestUri, StringComparison.Ordinal);
        Assert.DoesNotContain("slices=", requestUri, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task RunCompatibilityReindexAsync_WhenTerminalResponseHasVersionConflict_RejectsExactCopy()
    {
        var requestInvoker = new SequenceRequestInvoker(
            new StubResponse(200, "{\"task\":\"node:1\"}"),
            new StubResponse(200, """
                {
                  "completed": true,
                  "task": {
                    "node": "node",
                    "id": 1,
                    "type": "transport",
                    "action": "indices:data/write/reindex",
                    "status": {
                      "total": 1,
                      "created": 1,
                      "updated": 0,
                      "deleted": 0,
                      "noops": 0,
                      "version_conflicts": 1
                    },
                    "description": "reindex from [employees-v1] to [reindexed-v9-employees-v1]",
                    "start_time_in_millis": 1,
                    "running_time_in_nanos": 1,
                    "cancellable": true,
                    "cancelled": false,
                    "headers": {}
                  },
                  "response": {
                    "total": 1,
                    "created": 1,
                    "updated": 0,
                    "deleted": 0,
                    "noops": 0,
                    "version_conflicts": 1,
                    "failures": []
                  }
                }
                """));
        var client = new ElasticsearchClient(new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://localhost:9200")), requestInvoker));
        var runner = new ElasticReindexTaskRunner(client, TimeProvider.System);

        var exception = await Assert.ThrowsAsync<RepositoryException>(() => runner.RunCompatibilityReindexAsync(
            "employees-v1", "reindexed-v9-employees-v1", null, null, (_, _) => Task.CompletedTask, CancellationToken.None));

        Assert.Contains("Version conflicts: 1", exception.Message);
    }

    [Fact]
    public async Task RunCompatibilityReindexAsync_WhenTaskHasTerminalError_ReportsWireErrorWithoutBufferedBody()
    {
        var requestInvoker = new SequenceRequestInvoker(
            new StubResponse(200, "{\"task\":\"node:1\"}"),
            new StubResponse(200, """
                {
                  "completed": true,
                  "task": {
                    "node": "node",
                    "id": 1,
                    "type": "transport",
                    "action": "indices:data/write/reindex",
                    "status": { "total": 1 },
                    "description": "reindex from [employees-v1] to [reindexed-v9-employees-v1]",
                    "start_time_in_millis": 1,
                    "running_time_in_nanos": 1,
                    "cancellable": true,
                    "cancelled": false,
                    "headers": {}
                  },
                  "error": {
                    "type": "search_phase_execution_exception",
                    "reason": "source failed"
                  }
                }
                """));
        var client = new ElasticsearchClient(new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://localhost:9200")), requestInvoker));
        var runner = new ElasticReindexTaskRunner(client, TimeProvider.System);

        var exception = await Assert.ThrowsAnyAsync<RepositoryException>(() => runner.RunCompatibilityReindexAsync(
            "employees-v1", "reindexed-v9-employees-v1", null, null, (_, _) => Task.CompletedTask, CancellationToken.None));

        Assert.Contains("search_phase_execution_exception: source failed", exception.Message);
    }

    [Fact]
    public async Task CancelAndConfirmAsync_WhenCancelIsPartialAndTaskRemainsActive_ThrowsUncertain()
    {
        // Arrange
        var requestInvoker = new SequenceRequestInvoker(
            new StubResponse(200, """{"nodes":{},"node_failures":[{"type":"failed_node_exception","reason":"node unavailable"}]}"""),
            new StubResponse(200, """{"completed":false,"task":{"node":"node","id":1,"action":"indices:data/write/reindex","status":{"total":5,"created":1},"running_time_in_nanos":1,"cancellable":true,"cancelled":true,"headers":{}}}"""));
        var client = new ElasticsearchClient(new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://localhost:9200")), requestInvoker));

        // Act
        var exception = await Assert.ThrowsAsync<ElasticReindexTaskUncertainException>(() =>
            ElasticReindexTaskCancellation.CancelAndConfirmAsync(
                client,
                NullLogger.Instance,
                new TaskId("node:1"),
                "employees-v1",
                "reindexed-v9-employees-v1",
                new RepositoryException("original failure")));

        // Assert
        Assert.Contains("remained active", exception.InnerException?.Message);
        Assert.Contains("partial node or task failures", exception.InnerException?.Message);
    }

    [Fact]
    public Task CancelAndConfirmAsync_WhenCancelIsPartialButTaskIsGone_CompletesWithoutUncertainty()
    {
        // Arrange
        var requestInvoker = new SequenceRequestInvoker(
            new StubResponse(200, """{"nodes":{},"node_failures":[{"type":"failed_node_exception","reason":"task isn't running"}]}"""),
            new StubResponse(404, """{"error":{"type":"resource_not_found_exception","reason":"task node:1 isn't running and hasn't stored its results"},"status":404}"""));
        var client = new ElasticsearchClient(new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://localhost:9200")), requestInvoker));

        // Act & Assert
        return ElasticReindexTaskCancellation.CancelAndConfirmAsync(
            client,
            NullLogger.Instance,
            new TaskId("node:1"),
            "employees-v1",
            "reindexed-v9-employees-v1",
            new RepositoryException("original failure"));
    }

    [Fact]
    public async Task CancelAndConfirmAsync_WhenStatusCannotBeVerified_ThrowsUncertain()
    {
        // Arrange
        var requestInvoker = new SequenceRequestInvoker(
            new StubResponse(200, "{}"),
            new StubResponse(500, """{"error":{"type":"illegal_state_exception","reason":"status unavailable"},"status":500}"""));
        var client = new ElasticsearchClient(new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://localhost:9200")), requestInvoker));

        // Act
        var exception = await Assert.ThrowsAsync<ElasticReindexTaskUncertainException>(() =>
            ElasticReindexTaskCancellation.CancelAndConfirmAsync(
                client,
                NullLogger.Instance,
                new TaskId("node:1"),
                "employees-v1",
                "reindexed-v9-employees-v1",
                new RepositoryException("original failure")));

        // Assert
        Assert.Contains("Unable to verify termination", exception.InnerException?.Message);
    }

    [Fact]
    public Task CancelAndConfirmAsync_WhenTaskTerminatesAfterCleanCancel_CompletesWithoutUncertainty()
    {
        // Arrange
        var headers = new Dictionary<string, IEnumerable<string>> { ["x-elastic-product"] = ["Elasticsearch"] };
        var requestInvoker = new SequenceRequestInvoker(
            new StubResponse(200, "{}"),
            new StubResponse(404, """{"error":{"type":"resource_not_found_exception","reason":"task node:1 isn't running and hasn't stored its results"},"status":404}"""));
        var client = new ElasticsearchClient(new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://localhost:9200")), requestInvoker));

        // Act & Assert
        return ElasticReindexTaskCancellation.CancelAndConfirmAsync(
            client,
            NullLogger.Instance,
            new TaskId("node:1"),
            "employees-v1",
            "reindexed-v9-employees-v1",
            new RepositoryException("original failure"));
    }

    [Fact]
    public void GetOpaqueId_IsDeterministicAndLineageSpecific()
    {
        string first = ElasticReindexTaskRunner.GetOpaqueId("employees", "reindexed-v9-employees");
        string repeated = ElasticReindexTaskRunner.GetOpaqueId("employees", "reindexed-v9-employees");
        string differentSource = ElasticReindexTaskRunner.GetOpaqueId("employees-v2", "reindexed-v9-employees");
        string differentTarget = ElasticReindexTaskRunner.GetOpaqueId("employees", "reindexed-v9-employees-v2");

        Assert.Equal(first, repeated);
        Assert.NotEqual(first, differentSource);
        Assert.NotEqual(first, differentTarget);
        Assert.StartsWith("foundatio-compat-", first, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(0, IndexCompatibilityRecoveryAction.Reset)]
    [InlineData(1, IndexCompatibilityRecoveryAction.Wait)]
    [InlineData(2, IndexCompatibilityRecoveryAction.ManualIntervention)]
    public async Task InspectAsync_WithExactTaskIdentities_ReportsTaskCount(int exactTaskCount, IndexCompatibilityRecoveryAction expectedAction)
    {
        string opaqueId = ElasticReindexTaskRunner.GetOpaqueId("employees", "reindexed-v9-employees");
        var tasks = new Dictionary<string, object>(exactTaskCount);
        for (int id = 1; id <= exactTaskCount; id++)
        {
            tasks.Add($"node-1:{id}", new
            {
                node = "node-1", id, type = "transport", action = "indices:data/write/reindex",
                status = new { }, description = "Description is not task identity",
                start_time_in_millis = 1, running_time_in_nanos = 1, cancellable = true, cancelled = false,
                headers = new Dictionary<string, string> { [ElasticReindexTaskRunner.OpaqueIdHeader] = opaqueId }
            });
        }

        var requestInvoker = new SequenceRequestInvoker(
            new StubResponse(200, """{"version":{"number":"9.0.0"}}"""),
            new StubResponse(200, """
                {
                  "employees":{"aliases":{".foundatio-compatibility-upgrade":{"is_hidden":true}},"settings":{"index":{"blocks":{"write":"true"}}}},
                  "reindexed-v9-employees":{"aliases":{".foundatio-compatibility-upgrade":{"is_hidden":true}},"settings":{"index":{}}}
                }
                """),
            new StubResponse(200, """
                {"nodes":{"node-1":{"name":"node","transport_address":"127.0.0.1:9300","host":"host","ip":"127.0.0.1","roles":[],"attributes":{},"tasks":TASKS}}}
                """.Replace("TASKS", JsonSerializer.Serialize(tasks), StringComparison.Ordinal)));
        var client = new ElasticsearchClient(new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://localhost:9200")), requestInvoker));
        var recovery = new ElasticIndexCompatibilityRecovery(client, null, NullLogger.Instance);
        using var configuration = new ElasticConfiguration();
        using var index = new Index<object>(configuration, "employees");

        var status = await recovery.InspectAsync(index, "employees", TestContext.Current.CancellationToken);

        Assert.Equal(exactTaskCount, status.ActiveReindexTaskCount);
        Assert.Equal(expectedAction, status.Action);
    }

}
