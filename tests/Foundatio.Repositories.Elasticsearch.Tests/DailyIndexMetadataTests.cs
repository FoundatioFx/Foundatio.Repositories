using System;
using System.Linq;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Clients.Elasticsearch.Mapping;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Indexes;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Utility;
using Microsoft.Extensions.Time.Testing;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class DailyIndexMetadataTests : ElasticRepositoryTestBase
{
    public DailyIndexMetadataTests(ITestOutputHelper output) : base(output)
    {
    }

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        await RemoveDataAsync(false);
    }

    [Fact]
    public async Task GetIndexNamesRequest_ReturnsNamesAndAliasesWithoutMappingProperties()
    {
        var utcNow = new DateTime(2024, 1, 15, 0, 0, 0, DateTimeKind.Utc);
        _configuration.TimeProvider = new FakeTimeProvider(new DateTimeOffset(utcNow, TimeSpan.Zero));
        var index = new DailyEmployeeIndex(_configuration, 1);
        await index.DeleteAsync();

        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());
        await index.ConfigureAsync();
        IEmployeeRepository repository = new EmployeeRepository(index);

        await repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow), o => o.ImmediateConsistency());
        await repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow.AddDays(-2)), o => o.ImmediateConsistency());

        var pattern = (Indices)(IndexName)$"{index.Name}-v{index.Version}-*";
        var fullResponse = await _client.Indices.GetAsync(pattern, cancellationToken: TestCancellationToken);
        _logger.LogRequest(fullResponse);
        Assert.True(fullResponse.IsValidResponse);
        Assert.True(fullResponse.Indices.Count >= 2);
        Assert.Contains(fullResponse.Indices.Values, s => s.Mappings?.Properties is not null && s.Mappings.Properties.Any());

        var namesResponse = await _client.Indices.GetAsync(ElasticIndexExtensions.CreateGetIndexNamesRequest(pattern), cancellationToken: TestCancellationToken);
        _logger.LogRequest(namesResponse);
        Assert.True(namesResponse.IsValidResponse);

        Assert.Equal(SummarizeIndexAliases(fullResponse), SummarizeIndexAliases(namesResponse));
        Assert.All(namesResponse.Indices.Values, s => Assert.True(s.Mappings?.Properties is null || !s.Mappings.Properties.Any()));

        Assert.NotNull(namesResponse.ApiCallDetails?.Uri);
        string query = namesResponse.ApiCallDetails.Uri.Query;
        Assert.Contains("features=aliases", query);
        Assert.Contains("include_defaults=false", query);
    }

    [Fact]
    public async Task MappingResolver_AfterNamesOnlyIndexResolution_FetchesAndCachesLatestServerMapping()
    {
        var utcNow = new DateTime(2024, 1, 15, 0, 0, 0, DateTimeKind.Utc);
        _configuration.TimeProvider = new FakeTimeProvider(new DateTimeOffset(utcNow, TimeSpan.Zero));
        var index = new DailyEmployeeIndex(_configuration, 1);
        await index.DeleteAsync();

        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());
        await index.ConfigureAsync();
        IEmployeeRepository repository = new EmployeeRepository(index);

        await repository.AddAsync(EmployeeGenerator.Generate(createdUtc: utcNow), o => o.ImmediateConsistency());

        const string serverOnlyField = "serverOnlyKeyword";
        var putMappingResponse = await _client.Indices.PutMappingAsync<Employee>(m => m
            .Indices(index.GetIndex(utcNow))
            .Properties(p => p.Keyword(serverOnlyField)), TestCancellationToken);
        _logger.LogRequest(putMappingResponse);
        Assert.True(putMappingResponse.IsValidResponse);

        index.MappingResolver.RefreshMapping();

        var mapping = index.MappingResolver.GetMapping(serverOnlyField);
        Assert.NotNull(mapping);
        Assert.Equal(serverOnlyField, mapping.FullPath);
        Assert.IsType<KeywordProperty>(mapping.Property);

        var cachedMapping = index.MappingResolver.GetMapping(serverOnlyField);
        Assert.Same(mapping, cachedMapping);
    }

    private static string[] SummarizeIndexAliases(GetIndexResponse response)
    {
        return response.Indices
            .OrderBy(i => i.Key)
            .Select(i => $"{i.Key}:{(i.Value.Aliases is null ? String.Empty : String.Join(',', i.Value.Aliases.Keys.OrderBy(alias => alias)))}")
            .ToArray();
    }
}
