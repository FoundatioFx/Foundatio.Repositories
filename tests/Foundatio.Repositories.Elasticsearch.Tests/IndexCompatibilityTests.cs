using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Transport;
using Foundatio.Caching;
using Foundatio.Lock;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Exceptions;
using Foundatio.Utility;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public partial class IndexCompatibilityTests
{
    [Fact]
    public async Task ValidateAsync_WithDotPrefixedSource_RejectsBeforeMutation()
    {
        var requestInvoker = new SequenceRequestInvoker(
            new StubResponse(404, """{"error":{"type":"index_not_found_exception","reason":"no such index [reindexed-v9-.employees]"},"status":404}"""),
            new StubResponse(200, """{".employees":{"aliases":{},"mappings":{"_source":{"enabled":true}},"settings":{}}}"""),
            new StubResponse(200, """{".employees":{"settings":{}}}"""));
        var requestMethods = new List<string>();
        var settings = new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://localhost:9200")), requestInvoker)
            .OnRequestCompleted(call => requestMethods.Add(call.HttpMethod.ToString()));
        var client = new ElasticsearchClient(settings);
        var upgrader = new ElasticIndexCompatibilityUpgrader(client, TimeProvider.System);
        using var index = new Index<object>(new ElasticConfiguration(), ".employees");
        var compatibility = new IndexCompatibilityInfo
        {
            Name = index.Name,
            CreatedMajor = 8,
            ServerMajor = 9,
            ServerVersion = "9.0.0"
        };

        var exception = await Assert.ThrowsAsync<RepositoryException>(() =>
            upgrader.ValidateAsync(index, compatibility, CancellationToken.None));

        Assert.Contains("System or restricted index", exception.Message);
        Assert.Equal(["HEAD", "GET", "GET"], requestMethods);
    }

    [Fact]
    public void JsonDefinitionsMatch_IgnoresObjectPropertyOrder()
    {
        const string expected = """{"properties":{"name":{"type":"keyword","meta":{"first":"1","second":"2"}}}}""";
        const string actual = """{"properties":{"name":{"meta":{"second":"2","first":"1"},"type":"keyword"}}}""";

        Assert.True(ElasticIndexCompatibilityUpgrader.JsonDefinitionsMatch(expected, actual));
    }

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
    public async Task UpgradeAsync_WhenCreateFromOutcomeIsUnknown_RetainsTargetAndFailsClosed()
    {
        // Arrange
        var headers = new Dictionary<string, IEnumerable<string>> { ["x-elastic-product"] = ["Elasticsearch"] };
        var requestInvoker = new SequenceRequestInvoker(
            new StubResponse(404, """{"error":{"type":"index_not_found_exception","reason":"no such index [reindexed-v9-employees]"},"status":404}"""),
            new StubResponse(200, """{"employees":{"aliases":{},"mappings":{"_source":{"enabled":true}},"settings":{}}}"""),
            new StubResponse(200, """{"employees":{"settings":{}}}"""),
            new StubResponse(200, """{"acknowledged":true}"""),
            new StubResponse(200, """{"acknowledged":true,"shards_acknowledged":true,"indices":[{"name":"employees","blocked":true}]}"""),
            new StubResponse(200, """{"_shards":{"total":1,"successful":1,"failed":0}}"""),
            new StubResponse(500, "", new TimeoutException("The _create_from response was not received.")));
        var client = new ElasticsearchClient(new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://localhost:9200")), requestInvoker));
        var upgrader = new ElasticIndexCompatibilityUpgrader(client, TimeProvider.System);
        using var index = new Index<object>(new ElasticConfiguration(), "employees");
        var compatibility = new IndexCompatibilityInfo
        {
            Name = "employees",
            CreatedMajor = 8,
            ServerMajor = 9,
            ServerVersion = "9.0.0"
        };
        var locks = new ThrottlingLockProvider(new InMemoryCacheClient());
        await using var reindexLock = await locks.AcquireAsync("compatibility-upgrade", cancellationToken: TestContext.Current.CancellationToken);

        // Act
        var exception = await Assert.ThrowsAsync<RepositoryException>(() =>
            upgrader.UpgradeAsync(index, compatibility, reindexLock!, (_, _) => Task.CompletedTask, CancellationToken.None));

        Assert.Contains("recovery evidence could not be inspected", exception.Message);
        Assert.Contains("_create_from response was not received", exception.ToString());
    }

    [Fact]
    public async Task UpgradeAsync_WhenTargetCleanupCannotBeConfirmed_KeepsSourceWriteBlocked()
    {
        // Arrange
        var requestInvoker = new SequenceRequestInvoker(
            new StubResponse(404, """{"error":{"type":"index_not_found_exception","reason":"no such index [reindexed-v9-employees]"},"status":404}"""),
            new StubResponse(200, """{"employees":{"aliases":{},"mappings":{"_source":{"enabled":true}},"settings":{}}}"""),
            new StubResponse(200, """{"employees":{"settings":{}}}"""),
            new StubResponse(200, """{"acknowledged":true}"""),
            new StubResponse(200, """{"acknowledged":true,"shards_acknowledged":true,"indices":[{"name":"employees","blocked":true}]}"""),
            new StubResponse(200, """{"_shards":{"total":1,"successful":1,"failed":0}}"""),
            new StubResponse(200, """{"acknowledged":true,"shards_acknowledged":true,"index":"reindexed-v9-employees"}"""),
            new StubResponse(200, """{"acknowledged":true}"""),
            new StubResponse(200, """{"reindexed-v9-employees":{"aliases":{"unexpected":{}},"mappings":{"_source":{"enabled":true}},"settings":{}}}"""),
            new StubResponse(200, """{"reindexed-v9-employees":{"settings":{}}}"""),
            new StubResponse(500, """{"error":{"type":"master_not_discovered_exception","reason":"delete outcome unknown"},"status":500}"""),
            new StubResponse(200, """{"acknowledged":true}"""));
        var requestPaths = new List<string>();
        var settings = new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://localhost:9200")), requestInvoker)
            .OnRequestCompleted(call => requestPaths.Add($"{call.HttpMethod} {call.Uri?.AbsolutePath}"));
        var client = new ElasticsearchClient(settings);
        var upgrader = new ElasticIndexCompatibilityUpgrader(client, TimeProvider.System);
        using var index = new Index<object>(new ElasticConfiguration(), "employees");
        var compatibility = new IndexCompatibilityInfo
        {
            Name = "employees",
            CreatedMajor = 8,
            ServerMajor = 9,
            ServerVersion = "9.0.0"
        };
        var locks = new ThrottlingLockProvider(new InMemoryCacheClient());
        await using var reindexLock = await locks.AcquireAsync("compatibility-upgrade", cancellationToken: TestContext.Current.CancellationToken);

        // Act
        await Assert.ThrowsAsync<RepositoryException>(() =>
            upgrader.UpgradeAsync(index, compatibility, reindexLock!, (_, _) => Task.CompletedTask, CancellationToken.None));

        // Assert: exposing the source to writes is unsafe while the destination may still exist.
        Assert.DoesNotContain("PUT /employees/_settings", requestPaths);
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

    [Theory]
    [InlineData(0, null)]
    [InlineData(-1, null)]
    [InlineData(null, 0f)]
    [InlineData(null, -1f)]
    [InlineData(null, float.PositiveInfinity)]
    [InlineData(null, float.NaN)]
    public void ValidateCompatibilityReindexOptions_WithInvalidThrottle_Throws(int? batchSize, float? requestsPerSecond)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => ElasticReindexTaskRunner.ValidateOptions(batchSize, requestsPerSecond));
    }

    [Fact]
    public async Task ValidateCompatibilityAsync_WithInvalidThrottle_ThrowsBeforeElasticsearchRequest()
    {
        // Arrange
        var requestInvoker = new InMemoryRequestInvoker([], 500, null, "application/json");
        var client = new ElasticsearchClient(new ElasticsearchClientSettings(requestInvoker));
        var upgrader = new ElasticIndexCompatibilityUpgrader(client, TimeProvider.System);
        using var index = new Index<object>(new ElasticConfiguration(), "employees") { ReindexBatchSize = 0 };
        var compatibility = new IndexCompatibilityInfo
        {
            Name = "employees",
            CreatedMajor = 8,
            ServerMajor = 9,
            ServerVersion = "9.0.0"
        };

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => upgrader.ValidateAsync(index, compatibility, CancellationToken.None));
    }

    [Theory]
    [InlineData(9, 9, IndexCompatibilityState.Current)]
    [InlineData(8, 9, IndexCompatibilityState.RequiresReindex)]
    [InlineData(7, 9, IndexCompatibilityState.Unsupported)]
    [InlineData(10, 9, IndexCompatibilityState.Unsupported)]
    public void IndexCompatibilityInfo_State_IsDerivedFromMajorVersions(int createdMajor, int serverMajor, IndexCompatibilityState expected)
    {
        // Arrange
        var compatibility = new IndexCompatibilityInfo
        {
            Name = "employees",
            CreatedMajor = createdMajor,
            ServerMajor = serverMajor,
            ServerVersion = $"{serverMajor}.0.0"
        };

        // Act
        IndexCompatibilityState state = compatibility.State;

        // Assert
        Assert.Equal(expected, state);
        Assert.Equal(expected is IndexCompatibilityState.RequiresReindex, compatibility.RequiresReindexBeforeNextMajorUpgrade);
    }

    [Theory]
    [InlineData("{\"acknowledged\":true,\"shards_acknowledged\":true,\"indices\":[{\"name\":\"employees\",\"blocked\":true}]}", true)]
    [InlineData("{\"acknowledged\":false,\"shards_acknowledged\":true,\"indices\":[{\"name\":\"employees\",\"blocked\":true}]}", false)]
    [InlineData("{\"acknowledged\":true,\"shards_acknowledged\":false,\"indices\":[{\"name\":\"employees\",\"blocked\":true}]}", false)]
    [InlineData("{\"acknowledged\":true,\"shards_acknowledged\":true,\"indices\":[{\"name\":\"employees\",\"blocked\":false}]}", false)]
    [InlineData("{\"acknowledged\":true,\"shards_acknowledged\":true,\"indices\":[{\"name\":\"other\",\"blocked\":true}]}", false)]
    [InlineData("{\"acknowledged\":true,\"shards_acknowledged\":true,\"indices\":[]}", false)]
    public void IsWriteBlockConfirmed_RequiresExactFullyAcknowledgedSource(string responseBody, bool expected)
    {
        // Act
        bool confirmed = ElasticIndexCompatibilityUpgrader.IsWriteBlockConfirmed(responseBody, "employees");

        // Assert
        Assert.Equal(expected, confirmed);
    }

    [Fact]
    public async Task PutSettings_WithFlatOtherSettings_SerializesExplicitNullResets()
    {
        var headers = new Dictionary<string, IEnumerable<string>> { ["x-elastic-product"] = ["Elasticsearch"] };
        var requestInvoker = new InMemoryRequestInvoker(
            Encoding.UTF8.GetBytes("{\"acknowledged\":true}"), 200, null, "application/json", headers);
        byte[]? requestBody = null;
        var settings = new ElasticsearchClientSettings(requestInvoker)
            .DisableDirectStreaming()
            .OnRequestCompleted(call => requestBody = call.RequestBodyInBytes);
        var client = new ElasticsearchClient(settings);
        var nullValue = JsonSerializer.SerializeToElement<object?>(null);

        var response = await client.Indices.PutSettingsAsync("employees", d => d.Settings(new IndexSettings
        {
            OtherSettings = new Dictionary<string, object>
            {
                ["index.refresh_interval"] = nullValue,
                ["index.default_pipeline"] = nullValue,
                ["index.final_pipeline"] = nullValue
            }
        }), TestContext.Current.CancellationToken);

        Assert.True(response.IsValidResponse, response.GetErrorMessage());
        Assert.NotNull(requestBody);
        using var document = JsonDocument.Parse(requestBody);
        Assert.Equal(JsonValueKind.Null, document.RootElement.GetProperty("index.refresh_interval").ValueKind);
        Assert.Equal(JsonValueKind.Null, document.RootElement.GetProperty("index.default_pipeline").ValueKind);
        Assert.Equal(JsonValueKind.Null, document.RootElement.GetProperty("index.final_pipeline").ValueKind);
    }

    [Fact]
    public async Task GetSettings_WithFlatSettings_PreservesExplicitSettingPaths()
    {
        var headers = new Dictionary<string, IEnumerable<string>> { ["x-elastic-product"] = ["Elasticsearch"] };
        var requestInvoker = new InMemoryRequestInvoker(
            Encoding.UTF8.GetBytes("""
                {
                  "employees": {
                    "settings": {
                      "index.max_result_window": "12345",
                      "index.number_of_replicas": "1"
                    }
                  }
                }
                """), 200, null, "application/json", headers);
        var client = new ElasticsearchClient(new ElasticsearchClientSettings(requestInvoker));

        var response = await client.Indices.GetSettingsAsync((Indices)"employees",
            d => d.FlatSettings().IncludeDefaults(false), TestContext.Current.CancellationToken);

        Assert.True(response.IsValidResponse, response.GetErrorMessage());
        var state = response.RequireSingleResolvedIndexState("employees");
        var settings = state.Settings?.Index ?? state.Settings;
        Assert.NotNull(settings?.OtherSettings);
        Assert.Equal("12345", settings.OtherSettings["index.max_result_window"].ToString());
        Assert.Equal("1", settings.OtherSettings["index.number_of_replicas"].ToString());
    }

    [Fact]
    public async Task GetCurrentVersionAsync_WithHiddenDatedAlias_UsesAliasFromIndexMetadata()
    {
        const string name = "hidden-logs";
        string date = DateTime.UtcNow.ToString("yyyy.MM.dd");
        string responseBody = $$"""
            {
              "{{name}}-v1-{{date}}": { "aliases": {} },
              "{{name}}-v2-{{date}}": {
                "aliases": {
                  "{{name}}-{{date}}": { "is_hidden": true }
                }
              }
            }
            """;
        var requestInvoker = new SequenceRequestInvoker(
            new StubResponse(200, responseBody),
            new StubResponse(200, "{}"));
        using var configuration = new RequestInvokerElasticConfiguration(requestInvoker);
        using var index = new TestDailyIndex(configuration, name, 3);

        int currentVersion = await index.GetCurrentVersionAsync();

        Assert.Equal(2, currentVersion);
        Assert.Equal(1, configuration.RequestCount);
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
    [InlineData(1, IndexCompatibilityRecoveryAction.Wait)]
    [InlineData(2, IndexCompatibilityRecoveryAction.ManualIntervention)]
    public async Task InspectAsync_CountsOnlyTasksWithExactOpaqueId(int exactTaskCount, IndexCompatibilityRecoveryAction expectedAction)
    {
        string opaqueId = ElasticReindexTaskRunner.GetOpaqueId("employees", "reindexed-v9-employees");
        string secondOpaqueId = exactTaskCount is 2 ? opaqueId : "wrong";
        var requestInvoker = new SequenceRequestInvoker(
            new StubResponse(200, """
                {
                  "name":"node","cluster_name":"test","cluster_uuid":"test","version":{
                    "number":"9.0.0","build_flavor":"default","build_type":"unknown","build_hash":"unknown",
                    "build_date":"2026-01-01T00:00:00.000Z","build_snapshot":false,"lucene_version":"10.0.0",
                    "minimum_wire_compatibility_version":"8.0.0","minimum_index_compatibility_version":"8.0.0"
                  },"tagline":"test"
                }
                """),
            new StubResponse(200, """
                {
                  "employees":{"aliases":{".foundatio-compatibility-upgrade":{"is_hidden":true}},"settings":{"index":{"blocks":{"write":"true"}}}},
                  "reindexed-v9-employees":{"aliases":{".foundatio-compatibility-upgrade":{"is_hidden":true}},"settings":{"index":{}}}
                }
                """),
            new StubResponse(200, """
                {
                  "nodes":{"node-1":{"name":"node","transport_address":"127.0.0.1:9300","host":"host","ip":"127.0.0.1","roles":[],"attributes":{},"tasks":{
                    "node-1:1":{"node":"node-1","id":1,"type":"transport","action":"indices:data/write/reindex","status":{},"description":"reindex from [employees] to [reindexed-v9-employees]","start_time_in_millis":1,"running_time_in_nanos":1,"cancellable":true,"cancelled":false,"headers":{"X-Opaque-Id":"wrong"}},
                    "node-1:2":{"node":"node-1","id":2,"type":"transport","action":"indices:data/write/reindex","status":{},"description":"unrelated description","start_time_in_millis":1,"running_time_in_nanos":1,"cancellable":true,"cancelled":false,"headers":{"X-Opaque-Id":"OPAQUE_ID"}},
                    "node-1:3":{"node":"node-1","id":3,"type":"transport","action":"indices:data/write/reindex","status":{},"description":"same lineage","start_time_in_millis":1,"running_time_in_nanos":1,"cancellable":true,"cancelled":false,"headers":{"X-Opaque-Id":"SECOND_OPAQUE_ID"}}
                  }}}
                }
                """
                .Replace("SECOND_OPAQUE_ID", secondOpaqueId, StringComparison.Ordinal)
                .Replace("OPAQUE_ID", opaqueId, StringComparison.Ordinal)));
        var client = new ElasticsearchClient(new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://localhost:9200")), requestInvoker));
        var recovery = new ElasticIndexCompatibilityRecovery(client, null, NullLogger.Instance);
        using var index = new Index<object>(new ElasticConfiguration(), "employees");

        var status = await recovery.InspectAsync(index, "employees", TestContext.Current.CancellationToken);

        Assert.Equal(exactTaskCount, status.ActiveReindexTaskCount);
        Assert.Equal(expectedAction, status.Action);
    }

    [Fact]
    public void IsCompletedCutover_RequiresExactCleanCommittedTopology()
    {
        var completed = new IndexCompatibilityUpgradeStatus
        {
            IndexName = "employees",
            SourceIndex = "employees-v1",
            TargetIndex = "reindexed-v9-employees-v1",
            Action = IndexCompatibilityRecoveryAction.None,
            SourceExists = false,
            TargetExists = true,
            TargetHasCanonicalSourceAlias = true,
            ActiveReindexTaskCount = 0
        };

        Assert.True(ElasticIndexCompatibilityUpgrader.IsCompletedCutover(completed));
        Assert.False(ElasticIndexCompatibilityUpgrader.IsCompletedCutover(completed with { SourceExists = true }));
        Assert.False(ElasticIndexCompatibilityUpgrader.IsCompletedCutover(completed with { TargetWorkflowMarkerPresent = true }));
        Assert.False(ElasticIndexCompatibilityUpgrader.IsCompletedCutover(completed with { TargetWriteBlocked = true }));
        Assert.False(ElasticIndexCompatibilityUpgrader.IsCompletedCutover(completed with { TargetHasCanonicalSourceAlias = false }));
        Assert.False(ElasticIndexCompatibilityUpgrader.IsCompletedCutover(completed with { ActiveReindexTaskCount = null }));
        Assert.False(ElasticIndexCompatibilityUpgrader.IsCompletedCutover(completed with { Action = IndexCompatibilityRecoveryAction.ManualIntervention }));
    }

    [Fact]
    public void ShardsSucceeded_AcceptsUnassignedReplicasButRejectsFailures()
    {
        Assert.True(ElasticIndexCompatibilityUpgrader.ShardsSucceeded(new ShardStatistics { Failed = 0, Successful = 2, Total = 2 }));
        Assert.True(ElasticIndexCompatibilityUpgrader.ShardsSucceeded(new ShardStatistics { Failed = 0, Successful = 1, Total = 2 }));
        Assert.False(ElasticIndexCompatibilityUpgrader.ShardsSucceeded(new ShardStatistics { Failed = 1, Successful = 1, Total = 2 }));
        Assert.False(ElasticIndexCompatibilityUpgrader.ShardsSucceeded(new ShardStatistics { Failed = 0, Successful = 0, Total = 0 }));
        Assert.False(ElasticIndexCompatibilityUpgrader.ShardsSucceeded(null));
    }

    [Theory]
    [InlineData("employees", "employees")]
    [InlineData("employees-v1", "employees-v1")]
    [InlineData("reindexed-v8-employees", "employees")]
    [InlineData("reindexed-v9-employees-v1", "employees-v1")]
    [InlineData("reindexed-v8-logs-v1-2023.05.01", "logs-v1-2023.05.01")]
    [InlineData("reindexed-v-employees", "reindexed-v-employees")]
    [InlineData("reindexed-v0-employees", "reindexed-v0-employees")]
    [InlineData("reindexed-v08-employees", "reindexed-v08-employees")]
    public void CompatibilityIndexName_GetCanonicalName_ReturnsExpectedName(string name, string expected)
    {
        Assert.Equal(expected, CompatibilityIndexName.GetCanonicalName(name));
    }

    [Theory]
    [InlineData("employees", 8, "reindexed-v8-employees")]
    [InlineData("reindexed-v8-employees", 9, "reindexed-v9-employees")]
    [InlineData("reindexed-v9-employees-v1", 10, "reindexed-v10-employees-v1")]
    public void CompatibilityIndexName_Create_ReplacesExistingCompatibilityPrefix(string source, int serverMajor, string expected)
    {
        Assert.Equal(expected, CompatibilityIndexName.Create(source, serverMajor));
    }

    [Fact]
    public void CompatibilityIndexName_ConfiguredPrefixPreservesNaturalName()
    {
        // Arrange
        const string configuredName = "reindexed-v8-events";
        const string source = "reindexed-v8-events-v1";

        // Act
        string canonicalSource = CompatibilityIndexName.GetCanonicalName(source, configuredName);
        string target = CompatibilityIndexName.Create(source, 9, configuredName);
        string canonicalTarget = CompatibilityIndexName.GetCanonicalName(target, configuredName);

        // Assert
        Assert.Equal(source, canonicalSource);
        Assert.Equal("reindexed-v9-reindexed-v8-events-v1", target);
        Assert.Equal(source, canonicalTarget);
    }

    [Fact]
    public void CompatibilityIndexName_ConfiguredPrefixPreservesNaturalPlainName()
    {
        // Arrange
        const string configuredName = "reindexed-v8-events";

        // Act
        string canonicalName = CompatibilityIndexName.GetCanonicalName(configuredName, configuredName);
        string targetName = CompatibilityIndexName.Create(configuredName, 9, configuredName);

        // Assert
        Assert.Equal(configuredName, canonicalName);
        Assert.Equal("reindexed-v9-reindexed-v8-events", targetName);
    }

    [Theory]
    [InlineData("reindexed-v8-logs-2026.01.01", "reindexed-v8-logs-2026.01.01")]
    [InlineData("reindexed-v9-reindexed-v8-logs-2026.01.01", "reindexed-v8-logs-2026.01.01")]
    public void CompatibilityIndexName_ConfiguredPrefixPreservesNaturalChildName(string index, string expected)
    {
        // Arrange
        const string configuredName = "reindexed-v8-logs";

        // Act
        string canonicalName = CompatibilityIndexName.GetCanonicalName(index, configuredName);

        // Assert
        Assert.Equal(expected, canonicalName);
    }

    [Fact]
    public void CompatibilityIndexName_ConfiguredReindexedName_StripsGeneratedWrapper()
    {
        const string configuredName = "reindexed";
        const string source = "reindexed-v1";
        const string target = "reindexed-v9-reindexed-v1";

        Assert.Equal(source, CompatibilityIndexName.GetCanonicalName(source, configuredName));
        Assert.Equal(target, CompatibilityIndexName.Create(source, 9, configuredName));
        Assert.Equal(source, CompatibilityIndexName.GetCanonicalName(target, configuredName));
        Assert.Equal("reindexed-v10-reindexed-v1", CompatibilityIndexName.Create(target, 10, configuredName));
    }

    [Theory]
    [InlineData(null, "7.17.19", 7)]
    [InlineData(null, "8.11.0", 8)]
    [InlineData(null, "9.0.1", 9)]
    [InlineData(null, "9", 9)]
    [InlineData("7170199", null, 7)]
    [InlineData("8500003", null, 8)]
    [InlineData("9000000", null, 9)]
    [InlineData(null, null, null)]
    [InlineData("not-a-number", null, null)]
    [InlineData(null, "not-a-version", null)]
    [InlineData("7170199", "-1", 7)]
    [InlineData("9223372036854775807", null, null)]
    [InlineData("-1000000", null, null)]
    [InlineData("0", null, null)]
    public void ParseCreatedMajor_ParsesExpectedMajor(string? created, string? createdString, int? expectedMajor)
    {
        int? major = Foundatio.Repositories.Elasticsearch.Configuration.Index.ParseCreatedMajor(created, createdString);

        Assert.Equal(expectedMajor, major);
    }

    [Fact]
    public void ParseCreatedMajor_PrefersCreatedStringOverCreated()
    {
        // CreatedString ("8.x") should win even though the numeric Created id would parse to a different major.
        int? major = Foundatio.Repositories.Elasticsearch.Configuration.Index.ParseCreatedMajor("7170199", "8.11.0");

        Assert.Equal(8, major);
    }

    [Fact]
    public async Task GetIndexCompatibilityAsync_WithUnknownServerVersion_Throws()
    {
        using var configuration = new UnparseableVersionElasticConfiguration();
        using var index = new Index<object>(configuration, "employees");

        var exception = await Assert.ThrowsAsync<RepositoryException>(() => index.GetIndexCompatibilityAsync(TestContext.Current.CancellationToken));

        Assert.Contains("server version", exception.Message);
    }

    [Fact]
    public async Task UpgradeIndexCompatibilityAsync_RechecksCompatibilityInsideLock()
    {
        using var configuration = new ElasticConfiguration();
        using var index = new BecomesCompatibleIndex(configuration);
        configuration.AddIndex(index);
        await index.ConfigureAsync();
        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());

        await configuration.UpgradeIndexCompatibilityAsync([index], cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(2, index.CompatibilityChecks);
    }

    [Fact]
    public async Task UpgradeIndexCompatibilityAsync_WhenSourcesShareDestination_ThrowsBeforeMutation()
    {
        using var configuration = new ElasticConfiguration();
        using var index = new ConflictingDestinationIndex(configuration);
        configuration.AddIndex(index);

        var exception = await Assert.ThrowsAsync<RepositoryException>(() =>
            configuration.UpgradeIndexCompatibilityAsync([index], cancellationToken: TestContext.Current.CancellationToken));

        Assert.Contains("reindexed-v9-conflicting-destination-v1", exception.Message);
        Assert.Contains("conflicting-destination-v1", exception.Message);
        Assert.Contains("reindexed-v8-conflicting-destination-v1", exception.Message);
        Assert.Equal(1, index.CompatibilityChecks);
    }

    [Fact]
    public async Task UpgradeIndexCompatibilityAsync_WhenDetectionIsCanceled_PropagatesCancellation()
    {
        using var configuration = new ElasticConfiguration();
        using var index = new CanceledCompatibilityIndex(configuration);
        configuration.AddIndex(index);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => configuration.UpgradeIndexCompatibilityAsync([index], cancellationToken: TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task UpgradeIndexCompatibilityAsync_WithIndexFromDifferentConfiguration_ThrowsBeforeInspection()
    {
        using var owner = new ElasticConfiguration();
        using var other = new ElasticConfiguration();
        using var index = new CountingCompatibilityIndex(owner);
        owner.AddIndex(index);

        var exception = await Assert.ThrowsAsync<ArgumentException>(() => other.UpgradeIndexCompatibilityAsync([index], cancellationToken: TestContext.Current.CancellationToken));

        Assert.Contains("different Elasticsearch configuration", exception.Message);
        Assert.Equal(0, index.CompatibilityChecks);
    }

    [Fact]
    public async Task UpgradeIndexCompatibilityAsync_WhenRegisteredIndexesClaimSameSource_RejectsWholeBatchBeforeMutation()
    {
        using var configuration = new ElasticConfiguration();
        using var first = new StaticCompatibilityIndex(configuration, "employees", "shared-v1");
        using var second = new StaticCompatibilityIndex(configuration, "employees-archive", "shared-v1");
        configuration.AddIndex(first);
        configuration.AddIndex(second);

        var exception = await Assert.ThrowsAsync<RepositoryException>(() =>
            configuration.UpgradeIndexCompatibilityAsync([first, second], cancellationToken: TestContext.Current.CancellationToken));

        Assert.Contains("same compatibility source", exception.Message);
        Assert.Contains("shared-v1", exception.Message);
    }

    [Fact]
    public async Task UpgradeIndexCompatibilityAsync_WithLaterForeignIndex_ValidatesEntireBatchBeforeInspection()
    {
        using var configuration = new ElasticConfiguration();
        using var other = new ElasticConfiguration();
        using var first = new CountingCompatibilityIndex(configuration);
        using var foreign = new CountingCompatibilityIndex(other);
        configuration.AddIndex(first);
        other.AddIndex(foreign);

        var exception = await Assert.ThrowsAsync<ArgumentException>(() =>
            configuration.UpgradeIndexCompatibilityAsync([first, foreign], cancellationToken: TestContext.Current.CancellationToken));

        Assert.Contains("different Elasticsearch configuration", exception.Message);
        Assert.Equal(0, first.CompatibilityChecks);
        Assert.Equal(0, foreign.CompatibilityChecks);
    }

    [Fact]
    public async Task UpgradeIndexCompatibilityAsync_WithUnregisteredIndex_ThrowsBeforeInspection()
    {
        using var configuration = new ElasticConfiguration();
        using var index = new CountingCompatibilityIndex(configuration);

        var exception = await Assert.ThrowsAsync<ArgumentException>(() =>
            configuration.UpgradeIndexCompatibilityAsync([index], cancellationToken: TestContext.Current.CancellationToken));

        Assert.Contains("registered", exception.Message);
        Assert.Equal(0, index.CompatibilityChecks);
    }

    [Fact]
    public async Task UpgradeIndexCompatibilityAsync_WhenIndexSkippedMajor_ThrowsBeforeMutation()
    {
        using var configuration = new ElasticConfiguration();
        using var index = new UnsupportedCompatibilityIndex(configuration);
        configuration.AddIndex(index);

        var exception = await Assert.ThrowsAsync<RepositoryException>(() => configuration.UpgradeIndexCompatibilityAsync([index], cancellationToken: TestContext.Current.CancellationToken));

        Assert.Contains("one major at a time", exception.Message);
        Assert.Equal(1, index.CompatibilityChecks);
    }

    [Theory]
    [InlineData("employees*")]
    [InlineData("employees,other")]
    [InlineData("employees?")]
    public async Task InspectIndexCompatibilityUpgradeAsync_WithIndexExpression_RejectsBeforeRequest(string sourceIndex)
    {
        using var configuration = new ElasticConfiguration();
        using var index = new Index<object>(configuration, "employees");

        var exception = await Assert.ThrowsAsync<ArgumentException>(() =>
            configuration.InspectIndexCompatibilityUpgradeAsync(index, sourceIndex, TestContext.Current.CancellationToken));

        Assert.Contains("exact concrete source", exception.Message);
    }

    [Fact]
    public async Task InspectIndexCompatibilityUpgradeAsync_WithSourceOwnedByDifferentIndex_RejectsBeforeRequest()
    {
        using var configuration = new ElasticConfiguration();
        using var index = new Index<object>(configuration, "employees");

        var exception = await Assert.ThrowsAsync<ArgumentException>(() =>
            configuration.InspectIndexCompatibilityUpgradeAsync(index, "customers", TestContext.Current.CancellationToken));

        Assert.Contains("does not belong", exception.Message);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CompatibilityRecovery_WithSourceOwnedByRegisteredSibling_RejectsBeforeRequest(bool recover)
    {
        using var configuration = new ElasticConfiguration();
        using var events = new VersionedIndex<object>(configuration, "events", 1);
        using var natural = new VersionedIndex<object>(configuration, "reindexed-v8-events", 1);
        configuration.AddIndex(events);
        configuration.AddIndex(natural);

        var exception = recover
            ? await Assert.ThrowsAsync<ArgumentException>(() => configuration.RecoverIndexCompatibilityUpgradeAsync(
                events,
                natural.VersionedName,
                TestContext.Current.CancellationToken))
            : await Assert.ThrowsAsync<ArgumentException>(() => configuration.InspectIndexCompatibilityUpgradeAsync(
                events,
                natural.VersionedName,
                TestContext.Current.CancellationToken));

        Assert.Contains("does not belong", exception.Message);
    }

    [Fact]
    public void ValidateDistinctSourceAndTarget_WithCurrentMajorDestination_RejectsRecovery()
    {
        var exception = Assert.Throws<ArgumentException>(() =>
            ElasticIndexCompatibilityRecovery.ValidateDistinctSourceAndTarget("reindexed-v9-employees", "reindexed-v9-employees"));

        Assert.Contains("original pre-upgrade", exception.Message);
    }

    [Theory]
    [InlineData("employees", true)]
    [InlineData("employees-error", true)]
    [InlineData("reindexed-v9-employees", true)]
    [InlineData("customers", false)]
    [InlineData("employees-error2", false)]
    public void MatchesCompatibilitySource_ForPlainIndex_RequiresConfiguredCanonicalName(string sourceIndex, bool expected)
    {
        using var index = new Index<object>(new ElasticConfiguration(), "employees");
        IReadOnlyDictionary<string, Alias> aliases = sourceIndex.StartsWith("reindexed-v", StringComparison.Ordinal)
            ? new Dictionary<string, Alias> { ["employees"] = new() }
            : sourceIndex.EndsWith("-error", StringComparison.Ordinal)
                ? new Dictionary<string, Alias> { [ElasticReindexer.ErrorIndexOwnershipAlias] = new() { IsHidden = true } }
                : new Dictionary<string, Alias>();

        Assert.Equal(expected, index.MatchesCompatibilitySource(sourceIndex, aliases));
    }

    [Theory]
    [InlineData("employees-v1", true)]
    [InlineData("employees-v1-error", true)]
    [InlineData("reindexed-v9-employees-v2", true)]
    [InlineData("reindexed-v9-employees-v2-error", true)]
    [InlineData("employees-v1-other", false)]
    [InlineData("employees-v1-errors", false)]
    [InlineData("customers-v1", false)]
    public void MatchesCompatibilitySource_ForVersionedIndex_RequiresExactOwnedVersion(string sourceIndex, bool expected)
    {
        using var index = new VersionedIndex<object>(new ElasticConfiguration(), "employees", 2);
        string canonical = CompatibilityIndexName.GetCanonicalName(sourceIndex, index.Name);
        var aliases = new Dictionary<string, Alias>();
        if (!String.Equals(canonical, sourceIndex, StringComparison.Ordinal))
            aliases[canonical] = new();
        if (canonical.EndsWith("-error", StringComparison.Ordinal))
            aliases[ElasticReindexer.ErrorIndexOwnershipAlias] = new() { IsHidden = true };

        Assert.Equal(expected, index.MatchesCompatibilitySource(sourceIndex, aliases));
    }

    [Fact]
    public void ValidateCompatibilityUpgradeSource_ForRetainedInactiveVersion_AllowsOlderSchema()
    {
        using var index = new VersionedIndex<object>(new ElasticConfiguration(), "employees", 2);

        index.ValidateCompatibilityUpgradeSource("employees-v1", new Dictionary<string, Alias>());
    }

    [Fact]
    public void ValidateCompatibilityUpgradeSource_ForActiveOlderVersion_RequiresSchemaReindex()
    {
        using var index = new VersionedIndex<object>(new ElasticConfiguration(), "employees", 2);

        var exception = Assert.Throws<RepositoryException>(() => index.ValidateCompatibilityUpgradeSource(
            "employees-v1",
            new Dictionary<string, Alias> { ["employees"] = new() }));

        Assert.Contains("schema reindex", exception.Message);
    }

    [Theory]
    [InlineData("logs-v1-2026.08.11", true)]
    [InlineData("logs-v1-2026.08.11-error", true)]
    [InlineData("reindexed-v9-logs-v2-2026.08.11", true)]
    [InlineData("logs-v1-not-a-date", false)]
    [InlineData("logs-v1-not-a-date-error", false)]
    [InlineData("other-v1-2026.08.11", false)]
    public void MatchesCompatibilitySource_ForDailyIndex_RequiresOwnedDatedPartition(string sourceIndex, bool expected)
    {
        using var index = new DailyIndex<object>(new ElasticConfiguration(), "logs", 2);
        string canonical = CompatibilityIndexName.GetCanonicalName(sourceIndex, index.Name);
        var aliases = new Dictionary<string, Alias>();
        if (!String.Equals(canonical, sourceIndex, StringComparison.Ordinal))
            aliases[canonical] = new();
        if (canonical.EndsWith("-error", StringComparison.Ordinal))
            aliases[ElasticReindexer.ErrorIndexOwnershipAlias] = new() { IsHidden = true };

        Assert.Equal(expected, index.MatchesCompatibilitySource(sourceIndex, aliases));
    }

    [Fact]
    public void MatchesCompatibilitySource_WhenSiblingNativelyClaimsName_YieldsToSibling()
    {
        // Arrange
        using var configuration = new ElasticConfiguration();
        using var events = new VersionedIndex<object>(configuration, "events", 1);
        using var natural = new VersionedIndex<object>(configuration, "reindexed-v8-events", 1);
        configuration.AddIndex(events);
        configuration.AddIndex(natural);

        // Assert
        Assert.False(events.MatchesCompatibilitySource(natural.VersionedName, new Dictionary<string, Alias>()));
        Assert.True(natural.MatchesCompatibilitySource(natural.VersionedName, new Dictionary<string, Alias>()));
        const string repeatedlyWrappedSibling = "reindexed-v9-reindexed-v8-events-v1";
        var siblingAliases = new Dictionary<string, Alias> { [natural.VersionedName] = new() };
        Assert.False(events.MatchesCompatibilitySource(repeatedlyWrappedSibling, siblingAliases));
        Assert.True(natural.MatchesCompatibilitySource(repeatedlyWrappedSibling, siblingAliases));
        Assert.True(events.MatchesCompatibilitySource(events.VersionedName, new Dictionary<string, Alias>()));
        Assert.False(natural.MatchesCompatibilitySource(events.VersionedName, new Dictionary<string, Alias>()));
    }

    [Fact]
    public void MatchesCompatibilitySource_WhenAnotherInstanceUsesSameLogicalName_KeepsOwnership()
    {
        using var configuration = new ElasticConfiguration();
        using var registered = new VersionedIndex<object>(configuration, "events", 1);
        using var adHoc = new VersionedIndex<object>(configuration, "events", 1);
        configuration.AddIndex(registered);

        Assert.True(adHoc.MatchesCompatibilitySource(adHoc.VersionedName, new Dictionary<string, Alias>()));
    }

}
