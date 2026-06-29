using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests.Extensions;

public class ElasticIndexExtensionsTests
{
    [Fact]
    public void CreateGetIndexNamesRequest_RequestsOnlyAliasesFeature()
    {
        // Arrange & Act
        var request = ElasticIndexExtensions.CreateGetIndexNamesRequest(Indices.Parse("my-index-v1-*"));

        // Assert: requesting only the aliases feature makes Elasticsearch omit the (potentially huge)
        // mappings/settings sections, which is what prevents the OOM described in
        // https://github.com/elastic/elasticsearch-net/issues/8919.
        Assert.NotNull(request.Features);
        var feature = Assert.Single(request.Features);
        Assert.Equal(Feature.Aliases, feature);
    }

    [Fact]
    public void CreateGetIndexNamesRequest_DisablesIncludeDefaults()
    {
        // Arrange & Act
        var request = ElasticIndexExtensions.CreateGetIndexNamesRequest(Indices.Parse("my-index-v1-*"));

        // Assert
        Assert.False(request.IncludeDefaults);
    }

    [Fact]
    public void CreateGetIndexNamesRequest_PreservesRequestedIndices()
    {
        // Arrange & Act
        var request = ElasticIndexExtensions.CreateGetIndexNamesRequest(Indices.Parse("my-index-v1-*"));

        // Assert: the pattern must be preserved so name resolution still returns the same indexes.
        Assert.NotNull(request.Indices);
        Assert.Contains("my-index-v1-*", request.Indices.ToString());
    }

    [Fact]
    public void CreateGetIndexNamesRequest_WithoutIgnoreUnavailable_LeavesItUnset()
    {
        // Arrange & Act
        var request = ElasticIndexExtensions.CreateGetIndexNamesRequest(Indices.Parse("my-index-v1-*"));

        // Assert: omitting ignore_unavailable preserves the default behavior for callers that relied on it.
        Assert.Null(request.IgnoreUnavailable);
    }

    [Fact]
    public void CreateGetIndexNamesRequest_WithIgnoreUnavailable_SetsItTrue()
    {
        // Arrange & Act
        var request = ElasticIndexExtensions.CreateGetIndexNamesRequest(Indices.All, ignoreUnavailable: true);

        // Assert
        Assert.True(request.IgnoreUnavailable);
    }
}
