using System;
using System.Text.Json;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Utility;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests.Extensions;

public sealed class ElasticIndexExtensionsTests : ElasticRepositoryTestBase
{
    public ElasticIndexExtensionsTests(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task LimitToNamesAndAliases_WithMappedIndexes_OmitsMappingsAndSettings()
    {
        // Arrange
        string prefix = $"index-metadata-{Guid.NewGuid():N}";
        string alias = $"{prefix}-alias";
        string firstIndex = $"{prefix}-1";
        string secondIndex = $"{prefix}-2";
        var indices = Indices.Parse($"{prefix}-*");

        await using AsyncDisposableAction cleanup = new(async () =>
            await _client.Indices.DeleteAsync(indices, d => d.IgnoreUnavailable(), TestCancellationToken));

        var firstCreateResponse = await _client.Indices.CreateAsync(firstIndex, d => d
            .Aliases(a => a.Add(alias, _ => { }))
            .Mappings(m => m.Properties(p => p.Keyword("first"))), TestCancellationToken);
        var secondCreateResponse = await _client.Indices.CreateAsync(secondIndex, d => d
            .Aliases(a => a.Add(alias, _ => { }))
            .Mappings(m => m.Properties(p => p.Keyword("second"))), TestCancellationToken);

        Assert.True(firstCreateResponse.IsValidResponse);
        Assert.True(secondCreateResponse.IsValidResponse);

        // Act
        var response = await _client.Indices.GetAsync(indices, d => d.LimitToNamesAndAliases(), TestCancellationToken);

        // Assert
        Assert.True(response.IsValidResponse);
        Assert.Equal(2, response.Indices.Count);
        Assert.All(response.Indices.Values, state => Assert.True(state.Aliases?.ContainsKey(alias)));
        var requestUri = response.ApiCallDetails.Uri;
        Assert.NotNull(requestUri);
        Assert.Contains("features=aliases", requestUri.Query);
        Assert.Contains("include_defaults=false", requestUri.Query);

        Assert.NotNull(response.ApiCallDetails.ResponseBodyInBytes);
        using var responseBody = JsonDocument.Parse(response.ApiCallDetails.ResponseBodyInBytes);
        foreach (var indexState in responseBody.RootElement.EnumerateObject())
        {
            Assert.True(indexState.Value.TryGetProperty("aliases", out _));
            AssertEmptyObjectIfPresent(indexState.Value, "mappings");
            AssertEmptyObjectIfPresent(indexState.Value, "settings");
        }
    }

    private static void AssertEmptyObjectIfPresent(JsonElement indexState, string propertyName)
    {
        if (indexState.TryGetProperty(propertyName, out var value))
            Assert.Empty(value.EnumerateObject());
    }
}
