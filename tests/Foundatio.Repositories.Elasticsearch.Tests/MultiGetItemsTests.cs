using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Core.Get;
using Elastic.Clients.Elasticsearch.Core.MGet;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class MultiGetItemsTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ToMultiGetItems_WithItemHoldingNeitherDocumentNorError_ReturnsErrorItem(bool errorVariant)
    {
        // Arrange
        var responseItem = errorVariant
            ? new MultiGetResponseItem<Employee>((MultiGetError)null!)
            : new MultiGetResponseItem<Employee>((GetResult<Employee>)null!);
        var response = new MultiGetResponse<Employee> { Docs = [responseItem] };

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

    [Fact]
    public void ToMultiGetItems_WithMixedResults_PreservesOrderAndOutcomes()
    {
        // Arrange
        var document = new Employee { Id = "found" };
        var response = new MultiGetResponse<Employee>
        {
            Docs =
            [
                new MultiGetResponseItem<Employee>(new GetResult<Employee> { Id = "found", Index = "employees", Found = true, Source = document, PrimaryTerm = 2, SeqNo = 7, Routing = "company" }),
                new MultiGetResponseItem<Employee>(new GetResult<Employee> { Id = "missing", Index = "employees", Found = false }),
                new MultiGetResponseItem<Employee>(new MultiGetError { Id = "failed", Index = "employees", Error = new ErrorCause { Type = "unavailable_shards_exception", Reason = "unavailable" } }),
                new MultiGetResponseItem<Employee>(new MultiGetError { Id = "unknown", Index = "employees", Error = null! })
            ]
        };

        // Act
        var items = response.ToMultiGetItems();

        // Assert
        Assert.Collection(items,
            found =>
            {
                Assert.Equal("found", found.Id);
                Assert.Equal("employees", found.Index);
                Assert.True(found.IsFound);
                Assert.False(found.IsError);
                Assert.NotNull(found.Hit);
                Assert.Same(document, found.Hit.Document);
                Assert.Equal("company", found.Hit.Routing);
                Assert.Equal("2:7", found.Hit.Version);
                Assert.Equal(found.Hit.Version, document.Version);
            },
            missing =>
            {
                Assert.Equal("missing", missing.Id);
                Assert.Equal("employees", missing.Index);
                Assert.False(missing.IsFound);
                Assert.False(missing.IsError);
            },
            failed =>
            {
                Assert.Equal("failed", failed.Id);
                Assert.Equal("employees", failed.Index);
                Assert.False(failed.IsFound);
                Assert.True(failed.IsError);
                Assert.Equal("unavailable_shards_exception", failed.ErrorType);
                Assert.Equal("unavailable", failed.ErrorReason);
            },
            unknown =>
            {
                Assert.Equal("unknown", unknown.Id);
                Assert.Equal("employees", unknown.Index);
                Assert.False(unknown.IsFound);
                Assert.True(unknown.IsError);
                Assert.Equal("unknown", unknown.ErrorType);
                Assert.Null(unknown.ErrorReason);
            });
    }
}
