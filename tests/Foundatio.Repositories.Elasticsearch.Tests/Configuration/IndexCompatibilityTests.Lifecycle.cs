using System;
using System.Threading.Tasks;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Exceptions;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public partial class IndexCompatibilityTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task DeleteAsyncRejectsAnExactSiblingBeforeDispatchingAnyBatch(bool registerSiblingFirst)
    {
        // Arrange
        var invoker = new SequenceRequestInvoker(
            new(200, """{"events-v1":{"aliases":{"events":{}}}}""", Request: "GET /_alias/events"),
            new(200, """{"acknowledged":true}"""));
        using var configuration = new RequestInvokerElasticConfiguration(invoker);
        using var index = new VersionedIndex<object>(configuration, "events", 2);
        using var sibling = new Index<object>(configuration, "events-v2");
        configuration.AddIndex(registerSiblingFirst ? sibling : index);
        configuration.AddIndex(registerSiblingFirst ? index : sibling);

        // Act
        var error = await Assert.ThrowsAsync<RepositoryException>(() => index.DeleteAsync());

        // Assert
        Assert.Contains("events-v2", error.Message, StringComparison.Ordinal);
        Assert.Equal(["GET /_alias/events"], invoker.Requests);
        Assert.Equal(1, invoker.RemainingResponses);
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
}
