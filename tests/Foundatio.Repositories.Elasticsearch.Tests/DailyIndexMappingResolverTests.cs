using System;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch.Mapping;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Indexes;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Utility;
using Microsoft.Extensions.Time.Testing;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class DailyIndexMappingResolverTests : ElasticRepositoryTestBase
{
    public DailyIndexMappingResolverTests(ITestOutputHelper output) : base(output)
    {
    }

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        await RemoveDataAsync(false);
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
        Assert.NotNull(cachedMapping);
        Assert.Equal(serverOnlyField, cachedMapping.FullPath);
        Assert.IsType<KeywordProperty>(cachedMapping.Property);
    }
}
