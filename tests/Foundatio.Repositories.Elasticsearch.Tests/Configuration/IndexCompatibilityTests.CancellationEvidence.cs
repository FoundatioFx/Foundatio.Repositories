using System;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Foundatio.Repositories.Exceptions;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public partial class IndexCompatibilityTests
{
    private const string RunningEvidenceTask = """{"completed":false,"task":{"node":"node","id":1,"action":"indices:data/write/reindex","status":{"total":1,"created":0,"updated":0,"deleted":0,"noops":0,"version_conflicts":0},"running_time_in_nanos":1,"cancellable":true,"headers":{}}}""";
    private const string MissingEvidenceTask = """{"error":{"type":"resource_not_found_exception","reason":"task belongs to an unavailable node and no result is stored"},"status":404}""";

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task TaskResponse_Cancellation404IsNotPositiveTerminationEvidence(bool partialCancel)
    {
        // Arrange
        var original = new InvalidOperationException("Original copy failure");
        var invoker = new SequenceRequestInvoker(
            new StubResponse(200, CancelEvidenceResponse(partialCancel), Request: "POST /_tasks/node:1/_cancel"),
            new StubResponse(404, MissingEvidenceTask, Request: "GET /_tasks/node:1"));
        var client = CreateEvidenceClient(invoker);

        // Act
        var exception = await Assert.ThrowsAsync<ElasticReindexTaskUncertainException>(() =>
            ElasticReindexTaskCancellation.CancelAndConfirmAsync(client, NullLogger.Instance, new TaskId("node:1"),
                "employees", "reindexed-v9-employees", original));

        // Assert
        Assert.Contains("404", exception.ToString(), StringComparison.Ordinal);
        Assert.Contains("Original copy failure", exception.ToString(), StringComparison.Ordinal);
        Assert.Equal(0, invoker.RemainingResponses);
        Assert.Equal(2, invoker.Requests.Count);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task TaskResponse_CancellationAcceptsPositiveCompletedEvidence(bool partialCancel)
    {
        // Arrange
        var invoker = new SequenceRequestInvoker(
            new StubResponse(200, CancelEvidenceResponse(partialCancel), Request: "POST /_tasks/node:1/_cancel"),
            new StubResponse(200, RunningEvidenceTask.Replace("\"completed\":false", "\"completed\":true", StringComparison.Ordinal), Request: "GET /_tasks/node:1"));
        var client = CreateEvidenceClient(invoker);

        // Act
        await ElasticReindexTaskCancellation.CancelAndConfirmAsync(client, NullLogger.Instance, new TaskId("node:1"),
            "employees", "reindexed-v9-employees", new InvalidOperationException("Original failure"));

        // Assert
        Assert.Equal(0, invoker.RemainingResponses);
        Assert.Equal(2, invoker.Requests.Count);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task TaskResponse_Cancellation404DoesNotAuthorizeRunnerCleanup(bool partialCancel)
    {
        // Arrange
        var invoker = new SequenceRequestInvoker(
            new StubResponse(200, """{"task":"node:1"}""", Request: "POST /_reindex"),
            new StubResponse(200, RunningEvidenceTask, Request: "GET /_tasks/node:1"),
            new StubResponse(200, CancelEvidenceResponse(partialCancel), Request: "POST /_tasks/node:1/_cancel"),
            new StubResponse(404, MissingEvidenceTask, Request: "GET /_tasks/node:1"));
        var runner = new ElasticReindexTaskRunner(CreateEvidenceClient(invoker), TimeProvider.System);
        int terminated = 0;

        // Act
        await Assert.ThrowsAsync<ElasticReindexTaskUncertainException>(() => runner.RunCompatibilityReindexAsync(
            "employees", "reindexed-v9-employees", null, null,
            (_, _) => throw new OperationCanceledException("Stop copying"), () => terminated++, CancellationToken.None));

        // Assert
        Assert.Equal(0, terminated);
        Assert.Equal(0, invoker.RemainingResponses);
        Assert.Equal(4, invoker.Requests.Count);
    }

    [Fact]
    public async Task TaskResponse_Cancellation404PreservesCancellationAndArtifactsThroughUpgrade()
    {
        // Arrange
        var responses = CreateSafetySetupResponses();
        responses.Add(new(200, """{"task":"node:1"}""", Request: "POST /_reindex"));
        responses.Add(new(200, RunningEvidenceTask, Request: "GET /_tasks/node:1"));
        responses.Add(new(200, CancelEvidenceResponse(true), Request: "POST /_tasks/node:1/_cancel"));
        responses.Add(new(404, MissingEvidenceTask, Request: "GET /_tasks/node:1"));
        responses.AddRange(SafetyInspectionResponses());
        var fixture = CreatePreCutoverFailureFixture(responses);
        using var index = fixture.Index;
        using var configuration = index.Configuration;
        await using var reindexLock = await fixture.Locks.AcquireAsync("compatibility-upgrade", cancellationToken: TestContext.Current.CancellationToken);

        // Act
        var exception = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => fixture.Upgrader.UpgradeAsync(
            index, fixture.Compatibility, reindexLock, (_, message) =>
            {
                if (message?.StartsWith("[", StringComparison.Ordinal) is true)
                    throw new OperationCanceledException("Operator stopped the copy");
                return Task.CompletedTask;
            }, CancellationToken.None));

        // Assert
        Assert.Contains("Operator stopped the copy", exception.ToString(), StringComparison.Ordinal);
        Assert.Contains("404", exception.ToString(), StringComparison.Ordinal);
        Assert.DoesNotContain("DELETE /reindexed-v9-employees", fixture.Invoker.Requests);
        Assert.DoesNotContain("PUT /employees/_settings", fixture.Invoker.Requests);
        Assert.Equal(0, fixture.Invoker.RemainingResponses);
    }

    [Fact]
    public async Task TaskResponse_InspectionFailureDoesNotHideOriginalCancellation()
    {
        // Arrange
        var responses = CreateSafetySetupResponses();
        responses.RemoveRange(6, responses.Count - 6);
        responses.Add(new(500, """{"error":{"type":"master_not_discovered_exception","reason":"inspection unavailable"},"status":500}""", Request: "GET /"));
        var fixture = CreatePreCutoverFailureFixture(responses);
        using var index = fixture.Index;
        using var configuration = index.Configuration;
        await using var reindexLock = await fixture.Locks.AcquireAsync("compatibility-upgrade", cancellationToken: TestContext.Current.CancellationToken);

        // Act
        var exception = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => fixture.Upgrader.UpgradeAsync(
            index, fixture.Compatibility, reindexLock, (progress, _) =>
            {
                if (progress is 5)
                    throw new OperationCanceledException("Stop before destination creation");
                return Task.CompletedTask;
            }, CancellationToken.None));

        // Assert
        Assert.Contains("Stop before destination creation", exception.ToString(), StringComparison.Ordinal);
        Assert.Contains("inspection unavailable", exception.ToString(), StringComparison.Ordinal);
        Assert.DoesNotContain(fixture.Invoker.Requests, request => request.StartsWith("DELETE ", StringComparison.Ordinal));
        Assert.DoesNotContain("PUT /employees/_settings", fixture.Invoker.Requests);
        Assert.Equal(0, fixture.Invoker.RemainingResponses);
    }

    [Fact]
    public async Task TaskResponse_CleanupTimeoutDoesNotInventCallerCancellation()
    {
        // Arrange
        var responses = CreateSafetySetupResponses();
        responses.Add(new(200, """{"task":"node:1"}""", Request: "POST /_reindex"));
        responses.Add(new(200, RunningEvidenceTask.Replace("\"total\":1", "\"total\":null", StringComparison.Ordinal), Request: "GET /_tasks/node:1"));
        responses.Add(new(500, "", new OperationCanceledException("Independent cleanup timeout"), "POST /_tasks/node:1/_cancel"));
        responses.AddRange(SafetyInspectionResponses());
        var fixture = CreatePreCutoverFailureFixture(responses);
        using var index = fixture.Index;
        using var configuration = index.Configuration;
        await using var reindexLock = await fixture.Locks.AcquireAsync("compatibility-upgrade", cancellationToken: TestContext.Current.CancellationToken);

        // Act
        var exception = await Assert.ThrowsAnyAsync<RepositoryException>(() => fixture.Upgrader.UpgradeAsync(
            index, fixture.Compatibility, reindexLock, (_, _) => Task.CompletedTask, CancellationToken.None));

        // Assert
        Assert.Contains("Independent cleanup timeout", exception.ToString(), StringComparison.Ordinal);
        Assert.DoesNotContain("DELETE /reindexed-v9-employees", fixture.Invoker.Requests);
        Assert.DoesNotContain("PUT /employees/_settings", fixture.Invoker.Requests);
        Assert.Equal(0, fixture.Invoker.RemainingResponses);
    }

    private static ElasticsearchClient CreateEvidenceClient(SequenceRequestInvoker invoker)
    {
        return new ElasticsearchClient(new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://localhost:9200")), invoker));
    }

    private static string CancelEvidenceResponse(bool partial)
    {
        return partial
            ? """{"nodes":{},"node_failures":[{"type":"failed_node_exception","reason":"node disconnected"}]}"""
            : """{"nodes":{}}""";
    }
}
