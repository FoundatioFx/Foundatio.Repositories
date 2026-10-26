using System;
using System.Threading.Tasks;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class TestFixtureLifetimeTests(ITestOutputHelper output)
{
    [Fact]
    public async Task DisposeAsync_WithInitializedConfiguration_DisposesOwnedIndexes()
    {
        var fixture = new Fixture(output);

        await fixture.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => fixture.Index.EnsureIndexAsync(null));
    }

    private sealed class Fixture(ITestOutputHelper output) : ElasticRepositoryTestBase(output)
    {
        public IIndex Index => _configuration.Employees;
    }
}
