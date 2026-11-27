using System;
using System.Text.Json;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Foundatio.Parsers.ElasticQueries.Extensions;
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

        Assert.True(firstCreateResponse.IsValidResponse, firstCreateResponse.GetErrorMessage());
        Assert.True(secondCreateResponse.IsValidResponse, secondCreateResponse.GetErrorMessage());

        // Act
        var response = await _client.Indices.GetAsync(indices, d => d.LimitToNamesAndAliases(), TestCancellationToken);

        // Assert
        Assert.True(response.IsValidResponse, response.GetErrorMessage());
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

    [Fact]
    public async Task LimitToIndexSettings_WithMappedIndexes_ReturnsCreatedVersionWithoutMappings()
    {
        string prefix = $"index-settings-{Guid.NewGuid():N}";
        string firstIndex = $"{prefix}-1";
        string secondIndex = $"{prefix}-2";
        var indices = Indices.Parse($"{prefix}-*");

        await using AsyncDisposableAction cleanup = new(async () =>
            await _client.Indices.DeleteAsync(indices, d => d.IgnoreUnavailable(), TestCancellationToken));

        var firstCreateResponse = await _client.Indices.CreateAsync(firstIndex,
            d => d.Mappings(m => m.Properties(p => p.Keyword("first"))), TestCancellationToken);
        var secondCreateResponse = await _client.Indices.CreateAsync(secondIndex,
            d => d.Mappings(m => m.Properties(p => p.Keyword("second"))), TestCancellationToken);
        Assert.True(firstCreateResponse.IsValidResponse, firstCreateResponse.GetErrorMessage());
        Assert.True(secondCreateResponse.IsValidResponse, secondCreateResponse.GetErrorMessage());

        var response = await _client.Indices.GetAsync(indices, d => d.LimitToIndexSettings(), TestCancellationToken);

        Assert.True(response.IsValidResponse, response.GetErrorMessage());
        Assert.Equal(2, response.Indices.Count);
        Assert.All(response.Indices.Values, state => Assert.NotNull(state.Settings?.Index?.Version?.Created));
        var requestUri = response.ApiCallDetails.Uri;
        Assert.NotNull(requestUri);
        Assert.Contains("features=settings", requestUri.Query);
        Assert.Contains("include_defaults=false", requestUri.Query);

        Assert.NotNull(response.ApiCallDetails.ResponseBodyInBytes);
        using var responseBody = JsonDocument.Parse(response.ApiCallDetails.ResponseBodyInBytes);
        foreach (var indexState in responseBody.RootElement.EnumerateObject())
        {
            Assert.True(indexState.Value.TryGetProperty("settings", out _));
            AssertEmptyObjectIfPresent(indexState.Value, "mappings");
        }
    }

    [Fact]
    public async Task LimitToIndexCompatibility_ReturnsCreatedVersionAndAliasesWithoutMappings()
    {
        string name = $"index-compatibility-{Guid.NewGuid():N}";
        string alias = $"{name}-alias";
        await using AsyncDisposableAction cleanup = new(async () =>
            await _client.Indices.DeleteAsync(name, d => d.IgnoreUnavailable(), TestCancellationToken));
        var createResponse = await _client.Indices.CreateAsync(name, d => d
            .Aliases(a => a.Add(alias, _ => { }))
            .Mappings(m => m.Properties(p => p.Keyword("value"))), TestCancellationToken);
        Assert.True(createResponse.IsValidResponse, createResponse.GetErrorMessage());

        var response = await _client.Indices.GetAsync((Indices)name, d => d.LimitToIndexCompatibility(), TestCancellationToken);

        Assert.True(response.IsValidResponse, response.GetErrorMessage());
        var state = Assert.Single(response.Indices).Value;
        Assert.NotNull(state.Settings?.Index?.Version?.Created);
        Assert.True(state.Aliases?.ContainsKey(alias));
        Assert.NotNull(response.ApiCallDetails.Uri);
        Assert.Contains("features=", response.ApiCallDetails.Uri.Query);
        Assert.Contains("aliases", response.ApiCallDetails.Uri.Query);
        Assert.Contains("settings", response.ApiCallDetails.Uri.Query);
        Assert.NotNull(response.ApiCallDetails.ResponseBodyInBytes);
        using var responseBody = JsonDocument.Parse(response.ApiCallDetails.ResponseBodyInBytes);
        AssertEmptyObjectIfPresent(responseBody.RootElement.GetProperty(name), "mappings");
    }

    private static void AssertEmptyObjectIfPresent(JsonElement indexState, string propertyName)
    {
        if (indexState.TryGetProperty(propertyName, out var value))
            Assert.Empty(value.EnumerateObject());
    }
}
