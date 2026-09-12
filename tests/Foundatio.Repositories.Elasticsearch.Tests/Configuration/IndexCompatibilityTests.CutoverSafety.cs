using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Foundatio.Caching;
using Foundatio.Lock;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Exceptions;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public partial class IndexCompatibilityTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task InspectIndexCompatibilityUpgradeAsync_WithoutWorkflowMarkers_DoesNotHideActiveTask(bool sourceExists, bool taskActive)
    {
        string topology = sourceExists ? """{"employees":{"aliases":{},"settings":{}}}""" : "{}";
        string opaqueId = ElasticReindexTaskRunner.GetOpaqueId("employees", "reindexed-v9-employees");
        string tasks = taskActive ? """
            {"nodes":{"node":{"name":"node","transport_address":"localhost:9300","host":"localhost","ip":"127.0.0.1","roles":[],"attributes":{},"tasks":{
              "node:1":{"node":"node","id":1,"type":"transport","action":"indices:data/write/reindex","status":{},"start_time_in_millis":1,"running_time_in_nanos":1,"cancellable":true,"headers":{"X-Opaque-Id":"OPAQUE_ID"}}
            }}}}
            """.Replace("OPAQUE_ID", opaqueId, StringComparison.Ordinal) : """{"nodes":{}}""";
        var invoker = new SequenceRequestInvoker(
            new StubResponse(200, SafetyInfo, Request: "GET /"),
            new StubResponse(200, topology, Request: "GET /employees,reindexed-v9-employees,.foundatio-compatibility-upgrade"),
            new StubResponse(200, tasks, Request: "GET /_tasks"));
        using var configuration = new RequestInvokerElasticConfiguration(invoker);
        using var index = new Index<object>(configuration, "employees");
        configuration.AddIndex(index);

        var status = await configuration.InspectIndexCompatibilityUpgradeAsync(index, "employees", TestContext.Current.CancellationToken);

        Assert.Equal(taskActive ? IndexCompatibilityRecoveryAction.ManualIntervention : IndexCompatibilityRecoveryAction.None, status.Action);
        Assert.Equal(taskActive ? 1 : 0, status.ActiveReindexTaskCount);
        Assert.False(status.CanRecover);
        Assert.Equal(0, invoker.RemainingResponses);
    }

    [Fact]
    public async Task GetIndexCompatibilityAsync_WithCanceledToken_ThrowsCancellationBeforeRequests()
    {
        var invoker = new SequenceRequestInvoker([]);
        using var configuration = new RequestInvokerElasticConfiguration(invoker);
        using var index = new Index<object>(configuration, "employees");
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => index.GetIndexCompatibilityAsync(cancellation.Token));

        Assert.Empty(invoker.Requests);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public async Task RunCompatibilityReindexAsync_WithUnrecognizedActiveStatus_ConfirmsCancellation(bool terminated, bool missingTask)
    {
        const string active = """{"completed":false,"task":{"node":"node","id":1,"action":"indices:data/write/reindex","status":{},"running_time_in_nanos":1,"cancellable":true,"headers":{}}}""";
        var invoker = new SequenceRequestInvoker(
            new StubResponse(200, """{"task":"node:1"}""", Request: "POST /_reindex"),
            new StubResponse(200, missingTask ? """{"completed":false}""" : active, Request: "GET /_tasks/node:1"),
            new StubResponse(200, """{"nodes":{}}""", Request: "POST /_tasks/node:1/_cancel"),
            new StubResponse(terminated ? 404 : 200, terminated ? """{"error":{"type":"resource_not_found_exception"},"status":404}""" : active, Request: "GET /_tasks/node:1"));
        var client = new ElasticsearchClient(new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://localhost:9200")), invoker));
        var runner = new ElasticReindexTaskRunner(client, TimeProvider.System);

        var exception = await Assert.ThrowsAnyAsync<RepositoryException>(() => runner.RunCompatibilityReindexAsync(
            "employees", "reindexed-v9-employees", null, null, (_, _) => Task.CompletedTask, () => { }, CancellationToken.None));

        if (terminated)
            Assert.Contains("unrecognized status", exception.Message);
        else
            Assert.IsType<ElasticReindexTaskUncertainException>(exception);
        Assert.Equal(0, invoker.RemainingResponses);
        Assert.Equal(4, invoker.Requests.Count);
    }

    [Theory]
    [InlineData("{\"includes\":[\"visible\"]}", true)]
    [InlineData("{\"excludes\":[\"secret\"]}", true)]
    [InlineData("{\"includes\":[\"*\"]}", true)]
    [InlineData("{\"includes\":[],\"excludes\":[]}", false)]
    [InlineData("{}", false)]
    public async Task ValidateAsync_WithSourceFilters_RejectsPrunedMappingsBeforeMutation(string source, bool rejected)
    {
        var invoker = new SequenceRequestInvoker(
            new StubResponse(404, "{}", Request: "HEAD /reindexed-v9-employees"),
            new StubResponse(200, """{"employees":{"aliases":{},"mappings":{"_source":SOURCE},"settings":{}}}""".Replace("SOURCE", source, StringComparison.Ordinal), Request: "GET /employees"),
            new StubResponse(200, """{"employees":{"settings":{}}}""", Request: "GET /employees/_settings"));
        var client = new ElasticsearchClient(new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://localhost:9200")), invoker));
        var upgrader = new ElasticIndexCompatibilityUpgrader(client, TimeProvider.System);
        using var configuration = new ElasticConfiguration();
        using var index = new Index<object>(configuration, "employees");
        var compatibility = new IndexCompatibilityInfo { Name = index.Name, CreatedMajor = 8, ServerMajor = 9, ServerVersion = "9.0.0" };

        if (rejected)
        {
            var exception = await Assert.ThrowsAsync<RepositoryException>(() => upgrader.ValidateAsync(index, compatibility, TestContext.Current.CancellationToken));
            Assert.Contains("_source", exception.Message);
        }
        else
        {
            await upgrader.ValidateAsync(index, compatibility, TestContext.Current.CancellationToken);
        }

        Assert.Equal(0, invoker.RemainingResponses);
        Assert.Equal(["HEAD /reindexed-v9-employees", "GET /employees", "GET /employees/_settings"], invoker.Requests);
    }

    [Theory]
    [InlineData(false, 255)]
    [InlineData(false, 256)]
    [InlineData(true, 255)]
    [InlineData(true, 256)]
    public async Task ValidateAsync_WithGeneratedName_EnforcesUtf8ByteLimitBeforeRequests(bool multibyte, int bytes)
    {
        string name = multibyte ? new string('é', 120) + new string('a', bytes - 253) : new string('a', bytes - 13);
        Assert.Equal(bytes, Encoding.UTF8.GetByteCount($"reindexed-v9-{name}"));
        var invoker = new SequenceRequestInvoker(
            new StubResponse(404, "{}"),
            new StubResponse(200, """{"NAME":{"aliases":{},"mappings":{},"settings":{}}}""".Replace("NAME", name, StringComparison.Ordinal)),
            new StubResponse(200, """{"NAME":{"settings":{}}}""".Replace("NAME", name, StringComparison.Ordinal)));
        var client = new ElasticsearchClient(new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://localhost:9200")), invoker));
        var upgrader = new ElasticIndexCompatibilityUpgrader(client, TimeProvider.System);
        using var configuration = new ElasticConfiguration();
        using var index = new Index<object>(configuration, name);
        var compatibility = new IndexCompatibilityInfo { Name = name, CreatedMajor = 8, ServerMajor = 9, ServerVersion = "9.0.0" };

        if (bytes > 255)
        {
            var exception = await Assert.ThrowsAsync<RepositoryException>(() => upgrader.ValidateAsync(index, compatibility, TestContext.Current.CancellationToken));
            Assert.Contains("255", exception.Message);
            Assert.Empty(invoker.Requests);
        }
        else
        {
            await upgrader.ValidateAsync(index, compatibility, TestContext.Current.CancellationToken);
            Assert.Equal(3, invoker.Requests.Count);
        }
    }

    private const string SafetyInfo = """{"version":{"number":"9.0.0"}}""";
    private const string SafetyMarkedTopology = """
        {"employees":{"aliases":{".foundatio-compatibility-upgrade":{"is_hidden":true}},"settings":{"index":{"blocks":{"write":true}}}},
         "reindexed-v9-employees":{"aliases":{".foundatio-compatibility-upgrade":{"is_hidden":true}},"settings":{}}}
        """;

    private static List<StubResponse> CreateSafetySetupResponses() =>
    [
        new(404, "{}", Request: "HEAD /reindexed-v9-employees"),
        new(200, """{"employees":{"aliases":{},"mappings":{},"settings":{}}}""", Request: "GET /employees"),
        new(200, """{"employees":{"settings":{}}}""", Request: "GET /employees/_settings"),
        new(200, """{"acknowledged":true}""", Request: "POST /_aliases"),
        new(200, """{"acknowledged":true,"shards_acknowledged":true,"indices":[{"name":"employees","blocked":true}]}""", Request: "PUT /employees/_block/write"),
        new(200, """{"_shards":{"total":1,"successful":1,"failed":0}}""", Request: "POST /employees/_refresh"),
        new(200, """{"acknowledged":true,"shards_acknowledged":true,"index":"reindexed-v9-employees"}""", Request: "PUT /_create_from/employees/reindexed-v9-employees"),
        new(200, """{"acknowledged":true}""", Request: "POST /_aliases"),
        new(200, """{"reindexed-v9-employees":{"aliases":{".foundatio-compatibility-upgrade":{"is_hidden":true}},"mappings":{},"settings":{}}}""", Request: "GET /reindexed-v9-employees"),
        new(200, """{"reindexed-v9-employees":{"settings":{}}}""", Request: "GET /reindexed-v9-employees/_settings")
    ];

    private static StubResponse[] SafetyInspectionResponses() =>
    [
        new(200, SafetyInfo, Request: "GET /"),
        new(200, SafetyMarkedTopology, Request: "GET /employees,reindexed-v9-employees,.foundatio-compatibility-upgrade"),
        new(200, """{"nodes":{}}""", Request: "GET /_tasks")
    ];

    [Fact]
    public async Task UpgradeAsync_WithLostCutoverResponseAndOldTopology_DoesNotReset()
    {
        const string marked = """{"INDEX":{"aliases":{".foundatio-compatibility-upgrade":{"is_hidden":true}},"mappings":{},"settings":{"index":{"blocks":{"write":true}}}}}""";
        const string shards = """{"_shards":{"total":1,"successful":1,"failed":0}}""";
        const string count = """{"count":0,"_shards":{"total":1,"successful":1,"failed":0}}""";
        var responses = CreateSafetySetupResponses();
        responses.AddRange([
            new(200, """{"task":"node:1"}""", Request: "POST /_reindex"),
            new(200, """{"completed":true,"task":{"node":"node","id":1,"status":{"total":0,"created":0,"updated":0,"deleted":0,"noops":0,"version_conflicts":0}},"response":{"total":0,"created":0,"updated":0,"deleted":0,"noops":0,"version_conflicts":0,"failures":[]}}""", Request: "GET /_tasks/node:1"),
            new(200, """{"acknowledged":true,"shards_acknowledged":true,"indices":[{"name":"reindexed-v9-employees","blocked":true}]}""", Request: "PUT /reindexed-v9-employees/_block/write"),
            new(200, shards, Request: "POST /reindexed-v9-employees/_refresh"),
            new(200, count, Request: "POST /employees/_count"),
            new(200, count, Request: "POST /reindexed-v9-employees/_count"),
            new(200, """{"acknowledged":true}""", Request: "PUT /reindexed-v9-employees/_settings"),
            new(200, """{"status":"green","timed_out":false}""", Request: "GET /_cluster/health/reindexed-v9-employees"),
            new(200, marked.Replace("INDEX", "reindexed-v9-employees", StringComparison.Ordinal), Request: "GET /reindexed-v9-employees"),
            new(200, """{"reindexed-v9-employees":{"settings":{"index.blocks.write":"true"}}}""", Request: "GET /reindexed-v9-employees/_settings"),
            new(200, marked.Replace("INDEX", "employees", StringComparison.Ordinal), Request: "GET /employees"),
            new(200, """{"employees":{"settings":{"index.blocks.write":"true"}}}""", Request: "GET /employees/_settings"),
            new(500, "", new TimeoutException("Cutover response lost"), "POST /_aliases"),
            new(200, SafetyMarkedTopology, Request: "GET /employees,reindexed-v9-employees")
        ]);
        responses.AddRange(SafetyInspectionResponses());
        var invoker = new SequenceRequestInvoker([.. responses]);
        var aliasBodies = new List<string>();
        var client = new ElasticsearchClient(new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://localhost:9200")), invoker)
            .DisableDirectStreaming()
            .OnRequestCompleted(call =>
            {
                if (call.Uri?.AbsolutePath is "/_aliases" && call.RequestBodyInBytes is not null)
                    aliasBodies.Add(Encoding.UTF8.GetString(call.RequestBodyInBytes));
            }));
        using var configuration = new ElasticConfiguration();
        using var index = new Index<object>(configuration, "employees");
        using var cache = new InMemoryCacheClient();
        var locks = new ThrottlingLockProvider(cache);
        await using var reindexLock = await locks.AcquireAsync("compatibility-upgrade", cancellationToken: TestContext.Current.CancellationToken);
        var upgrader = new ElasticIndexCompatibilityUpgrader(client, TimeProvider.System);
        var compatibility = new IndexCompatibilityInfo { Name = index.Name, CreatedMajor = 8, ServerMajor = 9, ServerVersion = "9.0.0" };

        var exception = await Assert.ThrowsAsync<RepositoryException>(() => upgrader.UpgradeAsync(index, compatibility, reindexLock, (_, _) => Task.CompletedTask, CancellationToken.None));

        Assert.True(exception.Message.Contains("ManualIntervention", StringComparison.Ordinal), exception.ToString());
        Assert.Equal(0, invoker.RemainingResponses);
        Assert.Equal(responses.Count, invoker.Requests.Count);
        Assert.Equal(3, aliasBodies.Count);
        Assert.Contains("\"remove_index\":{\"index\":\"employees\"}", aliasBodies[2]);
        Assert.Contains("\"alias\":\"employees\"", aliasBodies[2]);
        Assert.DoesNotContain("DELETE /reindexed-v9-employees", invoker.Requests);
        Assert.DoesNotContain("PUT /employees/_settings", invoker.Requests);
    }

    [Fact]
    public async Task UpgradeAsync_WithCanceledCutoverResponseAndOldTopology_SurfacesOperationCanceled()
    {
        const string marked = """{"INDEX":{"aliases":{".foundatio-compatibility-upgrade":{"is_hidden":true}},"mappings":{},"settings":{"index":{"blocks":{"write":true}}}}}""";
        const string shards = """{"_shards":{"total":1,"successful":1,"failed":0}}""";
        const string count = """{"count":0,"_shards":{"total":1,"successful":1,"failed":0}}""";
        var responses = CreateSafetySetupResponses();
        responses.AddRange([
            new(200, """{"task":"node:1"}""", Request: "POST /_reindex"),
            new(200, """{"completed":true,"task":{"node":"node","id":1,"status":{"total":0,"created":0,"updated":0,"deleted":0,"noops":0,"version_conflicts":0}},"response":{"total":0,"created":0,"updated":0,"deleted":0,"noops":0,"version_conflicts":0,"failures":[]}}""", Request: "GET /_tasks/node:1"),
            new(200, """{"acknowledged":true,"shards_acknowledged":true,"indices":[{"name":"reindexed-v9-employees","blocked":true}]}""", Request: "PUT /reindexed-v9-employees/_block/write"),
            new(200, shards, Request: "POST /reindexed-v9-employees/_refresh"),
            new(200, count, Request: "POST /employees/_count"),
            new(200, count, Request: "POST /reindexed-v9-employees/_count"),
            new(200, """{"acknowledged":true}""", Request: "PUT /reindexed-v9-employees/_settings"),
            new(200, """{"status":"green","timed_out":false}""", Request: "GET /_cluster/health/reindexed-v9-employees"),
            new(200, marked.Replace("INDEX", "reindexed-v9-employees", StringComparison.Ordinal), Request: "GET /reindexed-v9-employees"),
            new(200, """{"reindexed-v9-employees":{"settings":{"index.blocks.write":"true"}}}""", Request: "GET /reindexed-v9-employees/_settings"),
            new(200, marked.Replace("INDEX", "employees", StringComparison.Ordinal), Request: "GET /employees"),
            new(200, """{"employees":{"settings":{"index.blocks.write":"true"}}}""", Request: "GET /employees/_settings"),
            new(500, "", new OperationCanceledException("Cutover was canceled"), "POST /_aliases"),
            new(200, SafetyMarkedTopology, Request: "GET /employees,reindexed-v9-employees")
        ]);
        responses.AddRange(SafetyInspectionResponses());
        var invoker = new SequenceRequestInvoker([.. responses]);
        var client = new ElasticsearchClient(new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://localhost:9200")), invoker));
        using var configuration = new ElasticConfiguration();
        using var index = new Index<object>(configuration, "employees");
        using var cache = new InMemoryCacheClient();
        var locks = new ThrottlingLockProvider(cache);
        await using var reindexLock = await locks.AcquireAsync("compatibility-upgrade", cancellationToken: TestContext.Current.CancellationToken);
        var upgrader = new ElasticIndexCompatibilityUpgrader(client, TimeProvider.System);
        var compatibility = new IndexCompatibilityInfo { Name = index.Name, CreatedMajor = 8, ServerMajor = 9, ServerVersion = "9.0.0" };

        var exception = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => upgrader.UpgradeAsync(index, compatibility, reindexLock, (_, _) => Task.CompletedTask, CancellationToken.None));

        Assert.IsNotType<RepositoryException>(exception);
        Assert.Equal(0, invoker.RemainingResponses);
        Assert.Equal(responses.Count, invoker.Requests.Count);
        Assert.DoesNotContain("DELETE /reindexed-v9-employees", invoker.Requests);
        Assert.DoesNotContain("PUT /employees/_settings", invoker.Requests);
    }

    [Theory]
    [InlineData("create", false)]
    [InlineData("before-copy", false)]
    [InlineData("before-copy", true)]
    [InlineData("terminal-error", false)]
    [InlineData("cancel-confirmed", false)]
    [InlineData("cancel-uncertain", false)]
    public async Task UpgradeAsync_WithPreCutoverFailure_OnlyCleansUpAfterPositiveEvidence(string failure, bool deleteFails)
    {
        const string active = """{"completed":false,"task":{"node":"node","id":1,"action":"indices:data/write/reindex","status":{},"running_time_in_nanos":1,"cancellable":true,"headers":{}}}""";
        var responses = CreateSafetySetupResponses();
        bool canReset = failure is "before-copy" or "terminal-error" or "cancel-confirmed";
        if (failure is "create")
        {
            responses.RemoveRange(6, responses.Count - 6);
            responses.Add(new(500, "", new TimeoutException("Creation response lost"), "PUT /_create_from/employees/reindexed-v9-employees"));
        }
        else if (failure is not "before-copy")
        {
            responses.Add(new(200, """{"task":"node:1"}""", Request: "POST /_reindex"));
            if (failure is "terminal-error")
            {
                responses.Add(new(200, """{"completed":true,"task":{"node":"node","id":1,"status":{}},"error":{"type":"search_phase_execution_exception","reason":"copy failed"}}""", Request: "GET /_tasks/node:1"));
            }
            else
            {
                responses.Add(new(200, active, Request: "GET /_tasks/node:1"));
                responses.Add(new(200, """{"nodes":{}}""", Request: "POST /_tasks/node:1/_cancel"));
                responses.Add(new(canReset ? 404 : 200, canReset ? """{"error":{"type":"resource_not_found_exception"},"status":404}""" : active, Request: "GET /_tasks/node:1"));
            }
        }
        responses.AddRange(SafetyInspectionResponses());
        if (canReset)
        {
            responses.AddRange(SafetyInspectionResponses());
            responses.Add(new(deleteFails ? 500 : 200, deleteFails ? """{"error":{"type":"master_not_discovered_exception","reason":"delete outcome unknown"},"status":500}""" : """{"acknowledged":true}""", Request: "DELETE /reindexed-v9-employees"));
            if (!deleteFails)
            {
                responses.Add(new(404, "{}", Request: "HEAD /reindexed-v9-employees"));
                responses.Add(new(200, """{"acknowledged":true}""", Request: "PUT /employees/_settings"));
                responses.Add(new(200, """{"acknowledged":true}""", Request: "POST /_aliases"));
            }
        }
        var invoker = new SequenceRequestInvoker([.. responses]);
        var client = new ElasticsearchClient(new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://localhost:9200")), invoker));
        using var configuration = new ElasticConfiguration();
        using var index = new Index<object>(configuration, "employees");
        using var cache = new InMemoryCacheClient();
        var locks = new ThrottlingLockProvider(cache);
        await using var reindexLock = await locks.AcquireAsync("compatibility-upgrade", cancellationToken: TestContext.Current.CancellationToken);
        var upgrader = new ElasticIndexCompatibilityUpgrader(client, TimeProvider.System);
        var compatibility = new IndexCompatibilityInfo { Name = index.Name, CreatedMajor = 8, ServerMajor = 9, ServerVersion = "9.0.0" };

        // A callback exception no longer aborts the upgrade (ReportProgressAsync guards observer failures), so
        // "before-copy" must cancel instead; the bare rethrow surfaces that cancellation unless the reset itself fails.
        bool expectCancellation = failure is "before-copy" && !deleteFails;
        var exception = await Record.ExceptionAsync(() => upgrader.UpgradeAsync(index, compatibility, reindexLock, (progress, _) =>
        {
            if (failure is "before-copy" && progress is 10)
                throw new OperationCanceledException("Stop before copying");
            return Task.CompletedTask;
        }, CancellationToken.None));

        Assert.NotNull(exception);
        if (expectCancellation)
            Assert.IsType<OperationCanceledException>(exception);
        else
            Assert.IsAssignableFrom<RepositoryException>(exception);
        Assert.Equal(0, invoker.RemainingResponses);
        Assert.Equal(responses.Count, invoker.Requests.Count);
        Assert.Equal(canReset, invoker.Requests.Contains("DELETE /reindexed-v9-employees"));
        Assert.Equal(canReset && !deleteFails, invoker.Requests.Contains("PUT /employees/_settings"));
        if (!canReset)
            Assert.Contains("ManualIntervention", exception.Message);
        if (deleteFails)
            Assert.Contains("delete outcome unknown", exception.ToString());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task UpgradeAsync_WithUncertainSubmissionAndEmptyTaskList_RetainsBothArtifacts(bool timeout)
    {
        var responses = CreateSafetySetupResponses();
        responses.Add(new StubResponse(timeout ? 500 : 200, "{}", timeout ? new TimeoutException("Submission response lost") : null, "POST /_reindex"));
        responses.AddRange(SafetyInspectionResponses());
        var invoker = new SequenceRequestInvoker([.. responses]);
        var client = new ElasticsearchClient(new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://localhost:9200")), invoker));
        using var configuration = new ElasticConfiguration();
        using var index = new Index<object>(configuration, "employees");
        using var cache = new InMemoryCacheClient();
        var locks = new ThrottlingLockProvider(cache);
        await using var reindexLock = await locks.AcquireAsync("compatibility-upgrade", cancellationToken: TestContext.Current.CancellationToken);
        var upgrader = new ElasticIndexCompatibilityUpgrader(client, TimeProvider.System);
        var compatibility = new IndexCompatibilityInfo { Name = index.Name, CreatedMajor = 8, ServerMajor = 9, ServerVersion = "9.0.0" };

        var exception = await Assert.ThrowsAsync<RepositoryException>(() => upgrader.UpgradeAsync(index, compatibility, reindexLock, (_, _) => Task.CompletedTask, CancellationToken.None));

        Assert.True(exception.Message.Contains("ManualIntervention", StringComparison.Ordinal), exception + "\n" + String.Join('\n', invoker.Requests));
        Assert.IsType<ElasticReindexTaskUncertainException>(exception.InnerException);
        Assert.Equal(0, invoker.RemainingResponses);
        Assert.Equal(14, invoker.Requests.Count);
        Assert.DoesNotContain(invoker.Requests, r => r.StartsWith("DELETE", StringComparison.Ordinal) || r.Contains("PUT /employees/_settings", StringComparison.Ordinal));
    }
}
