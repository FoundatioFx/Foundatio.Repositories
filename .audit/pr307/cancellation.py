from pathlib import Path
import sys

ROOT = Path('src/Foundatio.Repositories.Elasticsearch')
TESTS = Path('tests/Foundatio.Repositories.Elasticsearch.Tests/Configuration')

def replace_once(text, old, new):
    if text.count(old) != 1:
        raise RuntimeError(f'Expected one occurrence of {old[:100]!r}, found {text.count(old)}')
    return text.replace(old, new)

if sys.argv[1] == 'tests':
    path = TESTS / 'IndexCompatibilityTests.CancellationEvidence.cs'
    if path.exists():
        raise RuntimeError('Refusing to overwrite existing cancellation-evidence tests')
    path.write_text(r'''using System;
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
''')
elif sys.argv[1] == 'fix':
    path = ROOT / 'Repositories/ElasticReindexTaskCancellation.cs'
    text = path.read_text()
    text = replace_once(text, '''                logger.LogRequest(status);
                return;
            }

            if (status.IsValidResponse)''', '''                logger.LogRequest(status);
                throw new RepositoryException(
                    $"Reindex task '{taskId.FullyQualifiedId}' returned HTTP 404 after cancellation; an unavailable owner node or missing stored result does not prove that the task terminated.");
            }

            if (status.IsValidResponse)''')
    path.write_text(text)

    path = ROOT / 'Configuration/ElasticIndexCompatibilityUpgrader.cs'
    text = path.read_text()
    begin = text.index('        catch (Exception upgradeException)')
    end = text.index('    internal static bool IsCompletedCutover(', begin)
    section = text[begin:end]
    section = replace_once(section, '''            using var recoveryCancellation = new CancellationTokenSource(TimeSpan.FromSeconds(30));''', '''            var originalCancellation = GetOriginalCancellation(upgradeException);
            Exception CreateRecoveryFailure(string message, Exception cause)
            {
                return originalCancellation is null
                    ? new RepositoryException(message, cause)
                    : new OperationCanceledException(message, cause, originalCancellation.CancellationToken);
            }

            using var recoveryCancellation = new CancellationTokenSource(TimeSpan.FromSeconds(30));''')
    section = section.replace('throw new RepositoryException(', 'throw CreateRecoveryFailure(')
    section = replace_once(section, '''            if (status.Action is IndexCompatibilityRecoveryAction.None)
                throw;

            if (upgradeException.GetBaseException() is OperationCanceledException)''', '''            if (originalCancellation is not null)''')
    section = replace_once(section, 'throw new OperationCanceledException(upgradeException.Message, upgradeException, cancellationToken);', 'throw new OperationCanceledException(upgradeException.Message, upgradeException, originalCancellation.CancellationToken);')
    section = replace_once(section, '''            throw CreateRecoveryFailure(
                $"Compatibility upgrade for '{sourceIndex}' failed and now requires recovery action '{status.Action}'.",
                upgradeException);''', '''            if (status.Action is IndexCompatibilityRecoveryAction.None)
                throw;

            throw CreateRecoveryFailure(
                $"Compatibility upgrade for '{sourceIndex}' failed and now requires recovery action '{status.Action}'.",
                upgradeException);''')
    helper = '''    private static OperationCanceledException? GetOriginalCancellation(Exception exception)
    {
        // Cleanup aggregates preserve the operation failure first. A later cleanup-token timeout must not
        // turn a non-cancellation failure into apparent operator cancellation.
        while (exception is not OperationCanceledException)
        {
            if (exception is AggregateException { InnerExceptions.Count: > 0 } aggregate)
                exception = aggregate.InnerExceptions[0];
            else if (exception.InnerException is { } inner)
                exception = inner;
            else
                return null;
        }

        return (OperationCanceledException)exception;
    }

'''
    path.write_text(text[:begin] + section + helper + text[end:])

    path = TESTS / 'IndexCompatibilityTests.Task.cs'
    text = path.read_text()
    text = text.replace('WhenCancelIsPartialButTaskIsGone_CompletesWithoutUncertainty', 'WhenCancelIsPartialButTaskIsCompleted_CompletesWithoutUncertainty')
    old = 'new StubResponse(404, """{"error":{"type":"resource_not_found_exception","reason":"task node:1 isn\'t running and hasn\'t stored its results"},"status":404}""")'
    new = 'new StubResponse(200, """{"completed":true,"task":{"node":"node","id":1,"status":{}}}""")'
    if text.count(old) != 2:
        raise RuntimeError(f'Expected two positive-termination fixtures; found {text.count(old)}')
    path.write_text(text.replace(old, new))

    path = TESTS / 'IndexCompatibilityTests.CutoverSafety.cs'
    text = path.read_text()
    old = '''new StubResponse(terminated ? 404 : 200, terminated ? """{"error":{"type":"resource_not_found_exception"},"status":404}""" : active, Request: "GET /_tasks/node:1")'''
    new = '''new StubResponse(200, terminated ? active.Replace("\\"completed\\":false", "\\"completed\\":true", StringComparison.Ordinal) : active, Request: "GET /_tasks/node:1")'''
    text = replace_once(text, old, new)
    start = text.index('    public async Task UpgradeAsync_WhenCopyIsCanceledAndConfirmed_ResetsCleanly()')
    finish = text.index('    [Fact]', start)
    segment = text[start:finish]
    segment = replace_once(segment, '''new(404, """{"error":{"type":"resource_not_found_exception"},"status":404}""", Request: "GET /_tasks/node:1")''', '''new(200, active.Replace("\\"completed\\":false", "\\"completed\\":true", StringComparison.Ordinal), Request: "GET /_tasks/node:1")''')
    text = text[:start] + segment + text[finish:]
    start = text.index('    public async Task UpgradeAsync_WhenCanceledBeforeCopyButResetDeleteFails_PreservesArtifacts()')
    finish = text.index('    [Fact]', start)
    segment = replace_once(text[start:finish], 'Assert.IsAssignableFrom<RepositoryException>(exception);', 'Assert.IsAssignableFrom<OperationCanceledException>(exception);')
    text = text[:start] + segment + text[finish:]
    path.write_text(text)
else:
    raise RuntimeError('Expected tests or fix')
