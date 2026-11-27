using System;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Exceptions;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public partial class IndexCompatibilityTests
{
    private const string ServerInfo = """{"version":{"number":"9.5.0"}}""";

    [Theory]
    [InlineData("plain", "events")]
    [InlineData("dynamic", "events")]
    [InlineData("versioned", "events-v1")]
    [InlineData("daily", "events-v1-2020.01.01")]
    [InlineData("monthly", "events-v1-2020.01")]
    [InlineData("custom", "tenant-a")]
    public async Task PreflightReadsAllOwnedPartitionsInTwoRequests(string kind, string physical)
    {
        // Arrange
        var metadata = """
            {"PHYSICAL":{"aliases":{},"settings":{"index":{"hidden":true,"version":{"created":"8000099"}}}}}
            """.Replace("PHYSICAL", physical, StringComparison.Ordinal);
        var invoker = new SequenceRequestInvoker(new(200, ServerInfo, Request: "GET /"), new(200, metadata));
        using var config = new RequestInvokerElasticConfiguration(invoker);
        using Configuration.Index index = kind switch
        {
            "dynamic" => new DynamicIndex<object>(config, "events"),
            "versioned" => new VersionedIndex<object>(config, "events", 3),
            "daily" => new DailyIndex<object>(config, "events", 3),
            "monthly" => new MonthlyIndex<object>(config, "events", 3),
            "custom" => new PreflightTenantIndex(config),
            _ => new Index<object>(config, "events")
        };
        config.AddIndex(index);

        // Act
        var result = Assert.Single(await ((IIndexCompatibility)index).GetIndexCompatibilityAsync(TestContext.Current.CancellationToken));

        // Assert
        Assert.Equal(physical, result.Name);
        Assert.Equal(IndexCompatibilityState.RequiresReindex, result.State);
        Assert.Equal(2, config.RequestCount);
        var uri = Assert.IsType<Uri>(config.LastRequestUri);
        string query = Uri.UnescapeDataString(uri.Query);
        Assert.Contains("expand_wildcards=all", query, StringComparison.Ordinal);
        Assert.Contains("features=aliases,settings", query, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("9000099", IndexCompatibilityState.Current)]
    [InlineData("7000099", IndexCompatibilityState.Unsupported)]
    public async Task PreflightClassifiesOtherCreationVersions(string created, IndexCompatibilityState expected)
    {
        // Arrange
        var metadata = """{"events":{"aliases":{},"settings":{"index":{"version":{"created":"CREATED"}}}}}"""
            .Replace("CREATED", created, StringComparison.Ordinal);
        var invoker = new SequenceRequestInvoker(new(200, ServerInfo), new(200, metadata));
        using var config = new RequestInvokerElasticConfiguration(invoker);
        using var index = new Index<object>(config, "events");

        // Act
        var result = Assert.Single(await index.GetIndexCompatibilityAsync(TestContext.Current.CancellationToken));

        // Assert
        Assert.Equal(expected, result.State);
    }

    [Fact]
    public async Task PreflightRejectsUnknownServerBeforeReadingMetadata()
    {
        // Arrange
        var invoker = new SequenceRequestInvoker(new StubResponse(200, """{"version":{"number":"unknown"}}"""));
        using var config = new RequestInvokerElasticConfiguration(invoker);
        using var index = new Index<object>(config, "events");

        // Act & Assert
        await Assert.ThrowsAsync<RepositoryException>(() => index.GetIndexCompatibilityAsync(TestContext.Current.CancellationToken));
        Assert.Single(invoker.Requests);
    }

    [Theory]
    [InlineData("events-v1", """{"index":{"version":{}}}""")]
    [InlineData("events-v1-error", """{"index":{"version":{"created":"8000099"}}}""")]
    public async Task PreflightRejectsMissingVersionOrUnauthenticatedErrorIndex(string physical, string settings)
    {
        // Arrange
        string metadata = """{"PHYSICAL":{"aliases":{},"settings":SETTINGS}}"""
            .Replace("PHYSICAL", physical, StringComparison.Ordinal)
            .Replace("SETTINGS", settings, StringComparison.Ordinal);
        var invoker = new SequenceRequestInvoker(new(200, ServerInfo), new(200, metadata));
        using var config = new RequestInvokerElasticConfiguration(invoker);
        using var index = new VersionedIndex<object>(config, "events", 1);

        // Act & Assert
        await Assert.ThrowsAsync<RepositoryException>(() => index.GetIndexCompatibilityAsync(TestContext.Current.CancellationToken));
        Assert.Equal(2, invoker.Requests.Count);
    }

    [Theory]
    [InlineData("events-v1", """{"events":{}}""", 1)]
    [InlineData("reindexed-v9-events-v1", """{"events":{},"events-v1":{}}""", 1)]
    [InlineData("reindexed-v9-events-v1", """{"events":{}}""", 0)]
    [InlineData("events-v1", """{"events":{"routing":"tenant"}}""", 0)]
    public async Task PreflightRecognizesOwnedSchemaBacking(string physical, string aliases, int expected)
    {
        string metadata = """{"PHYSICAL":{"aliases":ALIASES,"settings":{"index":{"version":{"created":"8000099"}}}}}"""
            .Replace("PHYSICAL", physical, StringComparison.Ordinal).Replace("ALIASES", aliases, StringComparison.Ordinal);
        var invoker = new SequenceRequestInvoker(new(200, ServerInfo), new(200, metadata));
        using var config = new RequestInvokerElasticConfiguration(invoker);
        using var index = new Index<object>(config, "events");
        var rows = await index.GetIndexCompatibilityAsync(TestContext.Current.CancellationToken);
        Assert.Equal(expected, rows.Count);
        Assert.All(rows, row => Assert.Equal(IndexCompatibilityState.RequiresReindex, row.State));
        Assert.Equal(2, invoker.Requests.Count);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task PreflightYieldsToExactRegisteredSibling(bool versionedSibling)
    {
        // Arrange
        var invoker = new SequenceRequestInvoker(new(200, ServerInfo), new(200, """
            {"events-v1":{"aliases":{},"settings":{"index":{"version":{"created":"8000099"}}}},
             "events-v2":{"aliases":{},"settings":{"index":{"version":{"created":"8000099"}}}}}
            """));
        using var config = new RequestInvokerElasticConfiguration(invoker);
        using var index = new VersionedIndex<object>(config, "events", 1);
        using Configuration.Index sibling = versionedSibling ? new VersionedIndex<object>(config, "events-v2") : new Index<object>(config, "events-v2");
        config.AddIndex(index);
        config.AddIndex(sibling);

        // Act
        var result = await index.GetIndexCompatibilityAsync(TestContext.Current.CancellationToken);

        // Assert
        Assert.Equal(["events-v1"], result.Select(item => item.Name));
    }

    [Fact]
    public async Task PreflightHonorsCustomOwnershipWhenSeveralIndexesShareAnAlias()
    {
        var invoker = new SequenceRequestInvoker(new(200, ServerInfo), new(200, """
            {"tenant-a":{"aliases":{"tenants":{}},"settings":{"index":{"version":{"created":"8000099"}}}},
             "tenant-b":{"aliases":{"tenants":{}},"settings":{"index":{"version":{"created":"8000099"}}}}}
            """));
        using var config = new RequestInvokerElasticConfiguration(invoker);
        using var index = new PreflightTenantIndex(config);
        var rows = await index.GetIndexCompatibilityAsync(TestContext.Current.CancellationToken);
        Assert.Equal("tenant-a", Assert.Single(rows).Name);
    }

    [Fact]
    public async Task PreflightRejectsMultipleBackingsOfAnOrdinarySchemaAlias()
    {
        var invoker = new SequenceRequestInvoker(new(200, ServerInfo), new(200, """
            {"events-v1":{"aliases":{"events":{}}},"foreign":{"aliases":{"events":{}}}}
            """));
        using var config = new RequestInvokerElasticConfiguration(invoker);
        using var index = new Index<object>(config, "events");
        var error = await Assert.ThrowsAsync<RepositoryException>(() => index.GetIndexCompatibilityAsync(TestContext.Current.CancellationToken));
        Assert.Contains("multiple", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task StartupConfigurationDoesNotInvokePreflight()
    {
        // Arrange
        using var config = new RequestInvokerElasticConfiguration(new SequenceRequestInvoker());
        using var index = new StartupProbeIndex(config);
        config.AddIndex(index);

        // Act
        await config.ConfigureIndexesAsync(beginReindexingOutdated: false);

        // Assert
        Assert.Equal(0, config.RequestCount);
    }

    [Fact]
    public async Task PreflightRejectsOverlappingCustomOwnership()
    {
        // Arrange
        var invoker = new SequenceRequestInvoker(new(200, ServerInfo), new(200, """
            {"tenant-a":{"aliases":{},"settings":{"index":{"version":{"created":"8000099"}}}}}
            """));
        using var config = new RequestInvokerElasticConfiguration(invoker);
        using var first = new PreflightTenantIndex(config, "first");
        using var second = new PreflightTenantIndex(config, "second");
        config.AddIndex(first);
        config.AddIndex(second);

        // Act & Assert
        await Assert.ThrowsAsync<RepositoryException>(() => first.GetIndexCompatibilityAsync(TestContext.Current.CancellationToken));
    }

    private sealed class PreflightTenantIndex(IElasticConfiguration configuration, string name = "tenants") : Index<object>(configuration, name)
    {
        protected override string GetCompatibilityIndexPattern() => "tenant-*";
        protected internal override bool IsNativeIndexName(ReadOnlySpan<char> physical) =>
            physical.Equals("tenant-a".AsSpan(), StringComparison.Ordinal);
    }

    private sealed class StartupProbeIndex(IElasticConfiguration configuration) : Index<object>(configuration, "events")
    {
        public override Task ConfigureAsync() => Task.CompletedTask;
        public override Task MaintainAsync(bool includeOptionalTasks = true) => Task.CompletedTask;
    }
}
