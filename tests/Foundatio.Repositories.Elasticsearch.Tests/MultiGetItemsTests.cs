using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Core.Get;
using Elastic.Clients.Elasticsearch.Core.MGet;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class MultiGetItemsTests
{
    [Fact]
    public void ToMultiGetItems_WithItemHoldingNeitherDocumentNorError_ReturnsErrorItem()
    {
        // Arrange
        var response = new MultiGetResponse<Employee> { Docs = [new MultiGetResponseItem<Employee>((GetResult<Employee>)null!)] };

        // Act
        var items = response.ToMultiGetItems();

        // Assert
        var item = Assert.Single(items);
        Assert.True(item.IsError);
        Assert.False(item.IsFound);
        Assert.Equal("unknown", item.ErrorType);
        Assert.Contains("neither a document nor an error", item.ErrorReason);
    }

    [Fact]
    public void ToMultiGetItems_WithMissingIndexError_ReturnsNotFoundItem()
    {
        // Arrange
        var response = new MultiGetResponse<Employee>
        {
            Docs = [new MultiGetResponseItem<Employee>(new MultiGetError { Id = "1", Index = "gone", Error = new Elastic.Clients.Elasticsearch.ErrorCause { Type = "index_not_found_exception", Reason = "missing" } })]
        };

        // Act
        var item = Assert.Single(response.ToMultiGetItems());

        // Assert
        Assert.False(item.IsError);
        Assert.False(item.IsFound);
        Assert.Equal("1", item.Id);
    }
}
