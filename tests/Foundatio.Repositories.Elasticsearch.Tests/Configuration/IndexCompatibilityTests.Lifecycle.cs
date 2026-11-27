using System;
using System.Threading.Tasks;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Exceptions;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public partial class IndexCompatibilityTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public async Task DeleteAsyncRejectsAnExactSiblingBeforeDispatchingAnyBatch(bool registerSiblingFirst, bool versionedSibling)
    {
        // Arrange
        var invoker = new SequenceRequestInvoker(
            new(200, """{"events-v1":{"aliases":{"events":{}}}}""", Request: "GET /events"),
            new(200, """{"acknowledged":true}"""));
        using var configuration = new RequestInvokerElasticConfiguration(invoker);
        using var index = new VersionedIndex<object>(configuration, "events", 2);
        using Configuration.Index sibling = versionedSibling ? new VersionedIndex<object>(configuration, "events-v2") : new Index<object>(configuration, "events-v2");
        configuration.AddIndex(registerSiblingFirst ? sibling : index);
        configuration.AddIndex(registerSiblingFirst ? index : sibling);

        // Act
        var error = await Assert.ThrowsAsync<RepositoryException>(() => index.DeleteAsync());

        // Assert
        Assert.Contains("events-v2", error.Message, StringComparison.Ordinal);
        Assert.Equal(["GET /events"], invoker.Requests);
        Assert.Equal(1, invoker.RemainingResponses);
    }

    [Fact]
    public async Task DeleteAsyncDoesNotDeleteForeignIndexMatchedByHiddenAlias()
    {
        var invoker = new SequenceRequestInvoker(new(200, """
            {"foreign":{"aliases":{"events-v1-2020.01.01":{"is_hidden":true}}},"events-v1-2020.01.02":{"aliases":{}}}
            """), new(200, """{"acknowledged":true}""", Request: "DELETE /events-v1-2020.01.02"));
        using var config = new RequestInvokerElasticConfiguration(invoker);
        using var index = new DailyIndex<object>(config, "events");
        await index.DeleteAsync();
        Assert.Equal(0, invoker.RemainingResponses);
    }

    [Theory]
    [InlineData("""{"reindexed-v9-events-v42":{"aliases":{"events":{}}}}""", "{}")]
    [InlineData("{}", """{"events-v99-archive":{"aliases":{}}}""")]
    public async Task GetCurrentVersionAsyncIgnoresUnownedLookalikes(string direct, string wildcard)
    {
        var invoker = new SequenceRequestInvoker(new(200, direct), new(200, wildcard));
        using var config = new RequestInvokerElasticConfiguration(invoker);
        using var index = new VersionedIndex<object>(config, "events", 1);
        Assert.Equal(1, await index.GetCurrentVersionAsync());
    }

    [Fact]
    public async Task DeleteAsyncUsesOneRequestForAnUnambiguousConcreteName()
    {
        // Arrange
        var invoker = new SequenceRequestInvoker(new StubResponse(200, """{"acknowledged":true}""", Request: "DELETE /events"));
        using var configuration = new RequestInvokerElasticConfiguration(invoker);
        using var index = new Index<object>(configuration, "events");
        configuration.AddIndex(index);

        // Act
        await index.DeleteAsync();

        // Assert
        Assert.Single(invoker.Requests);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task DeleteAsyncChecksThePhysicalOwnerBeforeRetryingAnAlias(bool registerSiblingFirst)
    {
        // Arrange
        var invoker = new SequenceRequestInvoker(
            new(400, """{"error":{"type":"illegal_argument_exception","reason":"index [events] matches an alias"},"status":400}""", Request: "DELETE /events"),
            new(200, """{"reindexed-v9-events":{"aliases":{"events":{}}}}""", Request: "GET /events"),
            new(200, """{"acknowledged":true}""", Request: "DELETE /reindexed-v9-events"));
        using var configuration = new RequestInvokerElasticConfiguration(invoker);
        using var index = new Index<object>(configuration, "events");
        using var sibling = new Index<object>(configuration, "reindexed-v9-events");
        configuration.AddIndex(registerSiblingFirst ? sibling : index);
        configuration.AddIndex(registerSiblingFirst ? index : sibling);

        // Act
        var error = await Assert.ThrowsAsync<RepositoryException>(() => index.DeleteAsync());

        // Assert
        Assert.Contains(sibling.Name, error.Message, StringComparison.Ordinal);
        Assert.Equal(["DELETE /events", "GET /events"], invoker.Requests);
        Assert.Equal(1, invoker.RemainingResponses);
    }

    [Fact]
    public async Task DeleteAsyncThroughAnOrdinaryAliasHonorsCustomPhysicalNaming()
    {
        // Arrange
        var invoker = new SequenceRequestInvoker(
            new(400, """{"error":{"type":"illegal_argument_exception","reason":"index [tenants] matches an alias"},"status":400}""", Request: "DELETE /tenants"),
            new(200, """{"tenant-a":{"aliases":{"tenants":{}}}}""", Request: "GET /tenants"),
            new(200, """{"acknowledged":true}""", Request: "DELETE /tenant-a"));
        using var configuration = new RequestInvokerElasticConfiguration(invoker);
        using var index = new PreflightTenantIndex(configuration);
        configuration.AddIndex(index);

        // Act
        await index.DeleteAsync();

        // Assert
        Assert.Equal(0, invoker.RemainingResponses);
    }
}
