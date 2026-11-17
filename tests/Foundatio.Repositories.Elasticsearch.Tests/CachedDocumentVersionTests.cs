using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using Elastic.Transport;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;
using Foundatio.Serializer;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class CachedDocumentVersionTests
{
    [Theory]
    [InlineData("GetById", null)]
    [InlineData("GetById", "3:42")]
    [InlineData("GetByIds", null)]
    [InlineData("GetByIds", "3:42")]
    [InlineData("GetByIdsIndividual", null)]
    [InlineData("GetByIdsIndividual", "3:42")]
    [InlineData("FindOne", null)]
    [InlineData("FindOne", "3:42")]
    [InlineData("Find", null)]
    [InlineData("Find", "3:42")]
    public async Task CachedRead_WithSerializedEnvelope_UsesMetadataForEveryModelContract(string operation, string? version)
    {
        await AssertCachedVersionAsync<PublicVersionDocument>(operation, version);
        await AssertCachedVersionAsync<IgnoredVersionDocument>(operation, version);
        var explicitDocument = await AssertCachedVersionAsync<ExplicitVersionDocument>(operation, version);
        Assert.Equal("business-2", explicitDocument.Version);
    }

    private static async Task<T> AssertCachedVersionAsync<T>(string operation, string? version) where T : class, IIdentity, IVersioned, new()
    {
        using var configuration = new TestConfiguration();
        using var repository = new TestRepository<T>(configuration);
        var original = new T { Id = "one", Version = "8:8" };
        var hit = new FindHit<T>(original.Id, original, 0, version);
        string cacheKey = operation is "GetByIdsIndividual" ? "one" : "cached";
        var options = new CommandOptions().Cache();
        if (operation is not "GetByIdsIndividual")
            options.CacheKey(cacheKey);

        if (operation is "Find")
            await repository.SeedAsync(cacheKey, RoundTrip(configuration, new FindResults<T>([hit], 1)));
        else
            await repository.SeedAsync<ICollection<FindHit<T>>>(cacheKey, RoundTrip(configuration, new List<FindHit<T>> { hit }));

        T? document = operation switch
        {
            "GetById" => await repository.GetByIdAsync("one", options),
            "GetByIds" or "GetByIdsIndividual" => (await repository.GetByIdsAsync(new Ids(["one"]), options)).Single(),
            "FindOne" => (await repository.FindOneAsync(new RepositoryQuery(), options)).Document,
            _ => (await repository.FindAsync(new RepositoryQuery(), options)).Documents.Single()
        };

        Assert.NotNull(document);
        Assert.NotSame(original, document);
        Assert.Equal(version, document.Version);
        Assert.Equal("8:8", original.Version);
        return document;
    }

    private static T RoundTrip<T>(ElasticConfiguration configuration, T value) where T : class =>
        Assert.IsType<T>(configuration.Serializer.Deserialize<T>(configuration.Serializer.SerializeToBytes(value)));

    private sealed class TestConfiguration : ElasticConfiguration
    {
        protected override IRequestInvoker CreateRequestInvoker() => new InMemoryRequestInvoker();
    }

    private sealed class TestRepository<T>(ElasticConfiguration configuration)
        : ElasticReadOnlyRepositoryBase<T>(new Index(configuration, "cached-version-tests")) where T : class, new()
    {
        public Task SeedAsync<TValue>(string key, TValue value) => Cache.SetAsync(key, value);
    }

    public sealed class PublicVersionDocument : IIdentity, IVersioned
    {
        public string Id { get; set; } = null!;
        public string? Version { get; set; } = "9:9";
    }

    public sealed class IgnoredVersionDocument : IIdentity, IVersioned
    {
        public string Id { get; set; } = null!;
        [JsonIgnore]
        public string? Version { get; set; } = "9:9";
    }

    public sealed class ExplicitVersionDocument : IIdentity, IVersioned
    {
        public string Id { get; set; } = null!;
        public string? Version { get; set; } = "business-2";
        string? IVersioned.Version { get; set; } = "9:9";
    }
}
