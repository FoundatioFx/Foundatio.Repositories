using System;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Utility;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class IndexCompatibilityDiscoveryTests : ElasticRepositoryTestBase
{
    public IndexCompatibilityDiscoveryTests(ITestOutputHelper output) : base(output) { }

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        await RemoveDataAsync(false);
    }

    [Fact]
    public async Task GetIndexCompatibilityAsync_ForVersionedIndex_ExcludesSuffixLookalikes()
    {
        string name = $"compat-discovery-versioned-{Guid.NewGuid():N}";
        using var index = new VersionedIndex<object>(_configuration, name, 1);
        string lookalikeIndex = $"{index.VersionedName}-other";
        await using AsyncDisposableAction _ = new(async () =>
            await _client.Indices.DeleteAsync(Indices.Parse($"{index.VersionedName},{lookalikeIndex}"), d => d.IgnoreUnavailable(), TestCancellationToken));
        await index.ConfigureAsync();
        var createResponse = await _client.Indices.CreateAsync(lookalikeIndex, cancellationToken: TestCancellationToken);
        Assert.True(createResponse.IsValidResponse);

        var compatibility = await index.GetIndexCompatibilityAsync(TestCancellationToken);

        var info = Assert.Single(compatibility);
        Assert.Equal(index.VersionedName, info.Name);
    }
}
