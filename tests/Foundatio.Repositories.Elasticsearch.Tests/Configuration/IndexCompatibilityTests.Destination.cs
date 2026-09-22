using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Exceptions;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public partial class IndexCompatibilityTests
{
    [Theory]
    [InlineData("plain", "events", "reindexed-v9-events")]
    [InlineData("versioned", "events", "reindexed-v9-events")]
    [InlineData("versioned", "events-v1", "reindexed-v9-events")]
    [InlineData("versioned", "events-v1-error", "reindexed-v9-events")]
    [InlineData("daily", "events-v1-2024.01.15", "reindexed-v9-events")]
    [InlineData("monthly", "events-v1-2024.01", "reindexed-v9-events")]
    [InlineData("custom", "events", "custom-reserved")]
    [InlineData("minimal", "events", "reindexed-v9-events")]
    [InlineData("window-alias", "events", "reporting")]
    public async Task UpgradeIndexCompatibilityAsync_WithRegisteredDestination_RejectsBeforeRequests(string kind, string source, string siblingName)
    {
        // Arrange
        var invoker = new SequenceRequestInvoker();
        using var configuration = new RequestInvokerElasticConfiguration(invoker);
        var index = CreateStaticCompatibilityIndex(configuration, source, source);
        string target = CompatibilityIndexName.Create(source, 9, index.Name);
        IIndex sibling = kind switch
        {
            "plain" => new Index<object>(configuration, siblingName),
            "versioned" => new VersionedIndex<object>(configuration, siblingName, 2),
            "daily" => new DailyIndex<object>(configuration, siblingName, 2),
            "monthly" => new MonthlyIndex<object>(configuration, siblingName, 2),
            "custom" => new ReservedDestinationIndex(configuration, siblingName, target),
            "minimal" => new MinimalIndex(configuration, siblingName),
            "window-alias" => new DailyIndex<object>(configuration, siblingName),
            _ => throw new ArgumentOutOfRangeException(nameof(kind))
        };
        if (kind is "window-alias")
            ((Configuration.DailyIndex)sibling).AddAlias(target);
        configuration.AddIndex(index);
        configuration.AddIndex(sibling);

        // Act: the sibling is deliberately outside the requested batch and has never been created.
        var exception = await Assert.ThrowsAsync<RepositoryException>(() =>
            configuration.UpgradeIndexCompatibilityAsync([index], cancellationToken: TestContext.Current.CancellationToken));

        // Assert
        Assert.Contains("reserved", exception.Message, StringComparison.Ordinal);
        Assert.Contains(target, exception.Message, StringComparison.Ordinal);
        Assert.Contains(sibling.Name, exception.Message, StringComparison.Ordinal);
        Assert.Empty(invoker.Requests);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task UpgradeIndexCompatibilityAsync_WithEmptySibling_RejectsRegardlessOfBatchOrRegistrationOrder(bool explicitSubset, bool siblingFirst)
    {
        // Arrange
        var invoker = new SequenceRequestInvoker();
        using var configuration = new RequestInvokerElasticConfiguration(invoker);
        var index = CreateStaticCompatibilityIndex(configuration, "events", "events");
        var sibling = new StubCompatibilityIndex(configuration, "reindexed-v9-events",
            _ => Task.FromResult<IReadOnlyCollection<IndexCompatibilityInfo>>([]));
        configuration.AddIndex(siblingFirst ? sibling : index);
        configuration.AddIndex(siblingFirst ? index : sibling);

        // Act
        var exception = await Assert.ThrowsAsync<RepositoryException>(() =>
            configuration.UpgradeIndexCompatibilityAsync(explicitSubset ? [index] : null, cancellationToken: TestContext.Current.CancellationToken));

        // Assert
        Assert.Contains("reserved", exception.Message, StringComparison.Ordinal);
        Assert.Contains(sibling.Name, exception.Message, StringComparison.Ordinal);
        Assert.Empty(invoker.Requests);
    }

    [Fact]
    public async Task UpgradeIndexCompatibilityAsync_WithNewlyConflictingCandidate_RevalidatesUnderLock()
    {
        // Arrange
        var invoker = new SequenceRequestInvoker(
            new StubResponse(404, "", Request: "HEAD /reindexed-v9-events"),
            new StubResponse(200, """{"events":{"aliases":{},"mappings":{},"settings":{}}}""", Request: "GET /events"),
            new StubResponse(200, """{"events":{"settings":{}}}""", Request: "GET /events/_settings"));
        using var configuration = new RequestInvokerElasticConfiguration(invoker);
        var index = new StubCompatibilityIndex(configuration, "events", check => Task.FromResult<IReadOnlyCollection<IndexCompatibilityInfo>>(
        [
            new IndexCompatibilityInfo { Name = "events", CreatedMajor = check is 1 ? 8 : 9, ServerMajor = check is 1 ? 9 : 10, ServerVersion = check is 1 ? "9.0.0" : "10.0.0" }
        ]));
        var sibling = new Index<object>(configuration, "reindexed-v10-events");
        configuration.AddIndex(index);
        configuration.AddIndex(sibling);

        // Act
        var exception = await Assert.ThrowsAsync<RepositoryException>(() =>
            configuration.UpgradeIndexCompatibilityAsync([index], cancellationToken: TestContext.Current.CancellationToken));

        // Assert: only the first, non-conflicting plan's read-only validation reached Elasticsearch.
        Assert.Contains("reserved", exception.Message, StringComparison.Ordinal);
        Assert.Contains(sibling.Name, exception.Message, StringComparison.Ordinal);
        Assert.Equal(2, index.CompatibilityChecks);
        Assert.Equal(["HEAD /reindexed-v9-events", "GET /events", "GET /events/_settings"], invoker.Requests);
        Assert.Equal(0, invoker.RemainingResponses);
    }

    [Theory]
    [InlineData("reindexed-v9-becomes-compatible-extra")]
    [InlineData("reindexed-v90-becomes-compatible")]
    public async Task UpgradeIndexCompatibilityAsync_WithSimilarButDistinctSibling_DoesNotReject(string siblingName)
    {
        // Arrange
        var invoker = new SequenceRequestInvoker(
            new StubResponse(404, "", Request: "HEAD /reindexed-v9-becomes-compatible"),
            new StubResponse(200, """{"becomes-compatible":{"aliases":{},"mappings":{},"settings":{}}}""", Request: "GET /becomes-compatible"),
            new StubResponse(200, """{"becomes-compatible":{"settings":{}}}""", Request: "GET /becomes-compatible/_settings"));
        using var configuration = new RequestInvokerElasticConfiguration(invoker);
        var index = CreateBecomesCompatibleIndex(configuration);
        configuration.AddIndex(index);
        configuration.AddIndex(new Index<object>(configuration, siblingName));

        // Act: another completed migration is observed when compatibility is re-read under the lock.
        await configuration.UpgradeIndexCompatibilityAsync([index], cancellationToken: TestContext.Current.CancellationToken);

        // Assert
        Assert.Equal(2, index.CompatibilityChecks);
        Assert.Equal(3, invoker.Requests.Count);
        Assert.Equal(0, invoker.RemainingResponses);
    }

    private sealed class ReservedDestinationIndex : Index<object>
    {
        private readonly string _physicalName;

        public ReservedDestinationIndex(IElasticConfiguration configuration, string name, string physicalName) : base(configuration, name)
        {
            _physicalName = physicalName;
        }

        protected internal override bool IsNativeIndexName(ReadOnlySpan<char> sourceIndex)
        {
            return sourceIndex.Equals(_physicalName.AsSpan(), StringComparison.Ordinal);
        }
    }
}
