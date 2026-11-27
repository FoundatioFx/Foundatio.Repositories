using System;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch.Mapping;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Exceptions;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public partial class IndexCompatibilityTests
{
    private sealed class CustomDateIndex : DailyIndex<object>
    {
        public CustomDateIndex(IElasticConfiguration configuration) : base(configuration, "logs", 1) { }
        protected override DateTime GetIndexDate(string name) => new(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        protected override int GetIndexVersion(string name) => 1;
        public async Task<int> CountDiscoveredIndexesAsync() => (await GetIndexesAsync()).Count;
        public TypeMapping? GetLatestMapping() => GetLatestIndexMapping();
    }

    [Fact]
    public async Task GetIndexesAsync_DoesNotDiscoverPhysicalIndexOwnedByRegisteredSiblingWithPrefixedName()
    {
        // Arrange
        var invoker = new SequenceRequestInvoker(new StubResponse(200, """
            {"events-v1":{"aliases":{"events":{}}},"events-v2-v1":{"aliases":{"events-v2":{}}}}
            """));
        using var configuration = new RequestInvokerElasticConfiguration(invoker);
        var events = new TestVersionedIndex(configuration, "events", 1);
        using var eventsV2 = new VersionedIndex<object>(configuration, "events-v2", 1);
        configuration.AddIndex(events);
        configuration.AddIndex(eventsV2);

        // Act
        var discovered = await events.GetDiscoveredIndexNamesAsync();

        // Assert
        Assert.Equal(["events-v1"], discovered);
    }

    [Fact]
    public async Task DeleteAsync_WithExactNameCollidingWithSiblingAlias_RejectsInsteadOfDeletingSibling()
    {
        // Arrange
        var invoker = new SequenceRequestInvoker(
            new StubResponse(400, """
                {"error":{"type":"illegal_argument_exception","reason":"index [events] matches an alias, specify the corresponding concrete indices instead."},"status":400}
                """),
            new StubResponse(200, """{"events-v2-v1":{"aliases":{"events":{}}}}"""));
        using var configuration = new RequestInvokerElasticConfiguration(invoker);
        using var events = new Index<object>(configuration, "events");
        using var eventsV2 = new VersionedIndex<object>(configuration, "events-v2", 1);
        configuration.AddIndex(events);
        configuration.AddIndex(eventsV2);

        // Act
        var exception = await Assert.ThrowsAsync<RepositoryException>(() => events.DeleteAsync());

        // Assert
        Assert.Contains("events-v2-v1", exception.Message, StringComparison.Ordinal);
        Assert.Equal(2, configuration.RequestCount);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task DeleteAsync_WithWrappedCanonicalNameOwnedBySibling_RejectsInsteadOfDeletingSibling(bool siblingFirst)
    {
        // Arrange
        var invoker = new SequenceRequestInvoker(
            new StubResponse(200, """{"reindexed-v9-events-v2":{"aliases":{"events":{},"events-v2":{}}}}""", Request: "GET /events"));
        using var configuration = new RequestInvokerElasticConfiguration(invoker);
        using var events = new VersionedIndex<object>(configuration, "events", 2);
        using var sibling = new Index<object>(configuration, "events-v2");
        configuration.AddIndex(siblingFirst ? sibling : events);
        configuration.AddIndex(siblingFirst ? events : sibling);

        // Act
        var exception = await Assert.ThrowsAsync<RepositoryException>(() => events.DeleteAsync());

        // Assert
        Assert.Contains("belongs to another registered index", exception.Message, StringComparison.Ordinal);
        Assert.Equal(["GET /events"], invoker.Requests);
        Assert.Equal(0, invoker.RemainingResponses);
    }

    [Fact]
    public async Task GetIndexesAsync_WithCustomNaming_UsesVirtualDateAndVersionParsers()
    {
        // Arrange
        var invoker = new SequenceRequestInvoker(new StubResponse(200, """
            {"logs-v1-2024.01.01-archive":{"aliases":{"logs-2024.01.01":{},"logs":{}}}}
            """));
        using var configuration = new RequestInvokerElasticConfiguration(invoker);
        using var index = new CustomDateIndex(configuration);
        configuration.AddIndex(index);

        // Act & Assert
        Assert.Equal(1, await index.CountDiscoveredIndexesAsync());
        Assert.Equal(1, configuration.RequestCount);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task NormalDiscovery_RequestsOpenAndHiddenButNotClosedIndexes(bool mapping)
    {
        // Arrange
        var invoker = new SequenceRequestInvoker(new StubResponse(200, "{}"));
        using var configuration = new RequestInvokerElasticConfiguration(invoker);
        using var index = new CustomDateIndex(configuration);

        // Act
        if (mapping)
            Assert.Null(index.GetLatestMapping());
        else
            Assert.Equal(0, await index.CountDiscoveredIndexesAsync());

        // Assert
        Assert.NotNull(configuration.LastRequestUri);
        Assert.Contains("expand_wildcards=open,hidden", Uri.UnescapeDataString(configuration.LastRequestUri.Query));
        Assert.Equal(1, configuration.RequestCount);
    }

    [Fact]
    public async Task DeleteAsync_WithWildcard_DoesNotRequestClosedIndexes()
    {
        // Arrange
        var invoker = new SequenceRequestInvoker(new StubResponse(200, "{}"));
        using var configuration = new RequestInvokerElasticConfiguration(invoker);
        using var index = new DailyIndex<object>(configuration, "employees", 1);

        // Act
        await index.DeleteAsync();

        // Assert
        Assert.NotNull(configuration.LastRequestUri);
        Assert.Contains("expand_wildcards=open,hidden", Uri.UnescapeDataString(configuration.LastRequestUri.Query));
        Assert.Equal(1, configuration.RequestCount);
    }

    [Fact]
    public void GetLatestIndexMapping_WithCustomNaming_UsesVirtualDateAndVersionParsers()
    {
        // Arrange
        var invoker = new SequenceRequestInvoker(
            new StubResponse(200, """{"logs-v1-2024.01.01-archive":{"aliases":{"logs-2024.01.01":{},"logs":{}}}}"""),
            new StubResponse(200, """{"logs-v1-2024.01.01-archive":{"mappings":{"properties":{"message":{"type":"keyword"}}}}}"""));
        using var configuration = new RequestInvokerElasticConfiguration(invoker);
        using var index = new CustomDateIndex(configuration);
        configuration.AddIndex(index);

        // Act
        var mapping = index.GetLatestMapping();

        // Assert
        Assert.NotNull(mapping);
        Assert.IsType<KeywordProperty>(mapping.Properties!["message"]);
        Assert.Equal(2, configuration.RequestCount);
    }

    [Theory]
    [InlineData("{}", 0)]
    [InlineData("{\"logs-v1-2024.01.01\":{}}", 1)]
    public async Task GetIndexesAsync_WithGeneratedName_StillRequiresCanonicalAlias(string aliases, int expectedCount)
    {
        // Arrange
        var invoker = new SequenceRequestInvoker(new StubResponse(200, """
            {"reindexed-v9-logs-v1-2024.01.01":{"aliases":ALIASES}}
            """.Replace("ALIASES", aliases, StringComparison.Ordinal)));
        using var configuration = new RequestInvokerElasticConfiguration(invoker);
        using var index = new CustomDateIndex(configuration);
        configuration.AddIndex(index);

        // Act & Assert
        Assert.Equal(expectedCount, await index.CountDiscoveredIndexesAsync());
        Assert.Equal(1, configuration.RequestCount);
    }
}
