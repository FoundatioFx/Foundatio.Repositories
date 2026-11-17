using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Elastic.Clients.Elasticsearch.Serialization;
using Elastic.Transport;
using Elastic.Transport.Extensions;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Serialization;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class SourceSerializerContractTests
{
    [Fact]
    public void Serialize_WithPublicVersion_PreservesTheModelContract()
    {
        using var configuration = new TestConfiguration<PublicVersionDocument>();
        var serializer = configuration.Client.ElasticsearchClientSettings.SourceSerializer;
        var document = new PublicVersionDocument
        {
            Id = "root",
            Version = "1:5",
            Child = new PublicVersionDocument { Id = "child", Version = "business-2" }
        };

        string json = serializer.SerializeToString(document);
        var result = Deserialize<PublicVersionDocument>(serializer, json);

        Assert.Equal(CreateBaseline(configuration).SerializeToString(document), json);
        Assert.Equal("1:5", result.Version);
        Assert.Equal("business-2", result.Child?.Version);
    }

    [Fact]
    public void Serialize_WithExplicitVersion_PreservesPublicBusinessVersions()
    {
        using var configuration = new TestConfiguration<ExplicitVersionDocument>();
        var serializer = configuration.Client.ElasticsearchClientSettings.SourceSerializer;
        var document = new ExplicitVersionDocument
        {
            Version = "business-7",
            Child = new ExplicitVersionDocument { Version = "business-8" }
        };
        ((IVersioned)document).Version = "1:5";
        ((IVersioned)document.Child).Version = "2:9";

        string json = serializer.SerializeToString(document);
        var result = Deserialize<ExplicitVersionDocument>(serializer, json);

        Assert.Equal(CreateBaseline(configuration).SerializeToString(document), json);
        Assert.Equal("business-7", result.Version);
        Assert.Equal("business-8", result.Child?.Version);
        Assert.Null(((IVersioned)result).Version);
        Assert.Null(((IVersioned)result.Child!).Version);
    }

    [Fact]
    public void Serialize_WithIgnoredVersion_UsesTheModelAttribute()
    {
        using var configuration = new TestConfiguration<IgnoredVersionDocument>();
        var serializer = configuration.Client.ElasticsearchClientSettings.SourceSerializer;
        var document = new IgnoredVersionDocument { Version = "1:5", Name = "document" };

        string json = serializer.SerializeToString(document);
        var result = Deserialize<IgnoredVersionDocument>(serializer, json);

        Assert.Equal(CreateBaseline(configuration).SerializeToString(document), json);
        using var parsed = JsonDocument.Parse(json);
        Assert.False(parsed.RootElement.TryGetProperty("version", out _));
        Assert.Null(result.Version);
        Assert.Equal("document", result.Name);
    }

    [Theory]
    [InlineData("property")]
    [InlineData("type")]
    [InlineData("options")]
    [InlineData("replace")]
    public void Deserialize_WithObjectCreationHandling_PreservesInitializedCollectionBehavior(string scope)
    {
        switch (scope)
        {
            case "property":
                AssertPopulation<PropertyPopulateDocument>();
                break;
            case "type":
                AssertPopulation<TypePopulateDocument>();
                break;
            case "options":
                AssertPopulation<PopulateDocument>(options => options.PreferredObjectCreationHandling = JsonObjectCreationHandling.Populate);
                break;
            default:
                AssertPopulation<ReplacePopulateDocument>(options => options.PreferredObjectCreationHandling = JsonObjectCreationHandling.Populate, replaces: true);
                break;
        }
    }

    private static void AssertPopulation<T>(Action<JsonSerializerOptions>? configure = null, bool replaces = false) where T : PopulateDocument
    {
        using var configuration = new TestConfiguration<T>(configure);
        configuration.AddIndex(new Index<PublicVersionDocument>(configuration));
        var serializer = configuration.Client.ElasticsearchClientSettings.SourceSerializer;
        const string json = """{"children":[{"id":"new","version":"business-2"}]}""";

        var result = Deserialize<T>(serializer, json);
        var baseline = Deserialize<T>(CreateBaseline(configuration, configure), json);

        Assert.Equal(baseline.Children.Count, result.Children.Count);
        Assert.Equal(replaces ? 1 : 2, result.Children.Count);
        Assert.Equal("new", result.Children[^1].Id);
        Assert.Equal("business-2", result.Children[^1].Version);
        if (!replaces)
            Assert.Equal("existing", result.Children[0].Id);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Serialize_WithReferenceHandling_PreservesTheConfiguredObjectGraph(bool preserve)
    {
        void Configure(JsonSerializerOptions options) => options.ReferenceHandler = preserve ? ReferenceHandler.Preserve : ReferenceHandler.IgnoreCycles;
        using var configuration = new TestConfiguration<ReferenceDocument>(Configure);
        var serializer = configuration.Client.ElasticsearchClientSettings.SourceSerializer;
        var document = new ReferenceDocument { Name = "root" };
        document.First = document.Second = new ReferenceDocument { Name = "child", Parent = document };

        string json = serializer.SerializeToString(document);
        var result = Deserialize<ReferenceDocument>(serializer, json);

        Assert.Equal(CreateBaseline(configuration, Configure).SerializeToString(document), json);
        Assert.NotNull(result.First);
        Assert.Equal("child", result.First.Name);
        if (preserve)
        {
            Assert.Same(result.First, result.Second);
            Assert.Same(result, result.First.Parent);
        }
        else
        {
            Assert.Null(result.First.Parent);
            Assert.Null(result.Second?.Parent);
        }
    }

    [Fact]
    public void Serialize_WithNonVersionedDocument_PreservesSharedReferences()
    {
        void Configure(JsonSerializerOptions options) => options.ReferenceHandler = ReferenceHandler.Preserve;
        using var configuration = new TestConfiguration<NonVersionedDocument>(Configure);
        var serializer = configuration.Client.ElasticsearchClientSettings.SourceSerializer;
        var child = new ReferenceDocument { Name = "shared" };
        var document = new NonVersionedDocument { First = child, Second = child };

        string json = serializer.SerializeToString(document);
        var result = Deserialize<NonVersionedDocument>(serializer, json);

        Assert.Equal(CreateBaseline(configuration, Configure).SerializeToString(document), json);
        Assert.NotNull(result.First);
        Assert.Same(result.First, result.Second);
    }

    [Fact]
    public void Serialize_WithGeneratedMetadata_PreservesVersionAndNumberHandling()
    {
        void Configure(JsonSerializerOptions options) => options.TypeInfoResolver = SourceSerializerGeneratedContext.Default;
        using var configuration = new TestConfiguration<GeneratedSourceDocument>(Configure);
        var serializer = configuration.Client.ElasticsearchClientSettings.SourceSerializer;
        var document = new GeneratedSourceDocument { Version = "1:5", Values = [3, 7] };

        string json = serializer.SerializeToString(document);
        var result = Deserialize<GeneratedSourceDocument>(serializer, json);

        Assert.Equal(CreateBaseline(configuration, Configure).SerializeToString(document), json);
        Assert.Equal("1:5", result.Version);
        Assert.Equal(document.Values, result.Values);
        using var parsed = JsonDocument.Parse(json);
        Assert.Equal(JsonValueKind.String, parsed.RootElement.GetProperty("values")[0].ValueKind);
    }

    private static Elastic.Transport.Serializer CreateBaseline(ElasticConfiguration configuration, Action<JsonSerializerOptions>? configure = null)
    {
        return new DefaultSourceSerializer(configuration.Client.ElasticsearchClientSettings, options =>
        {
            configure?.Invoke(options);
            options.ConfigureFoundatioRepositoryDefaults();
        });
    }

    private static T Deserialize<T>(Elastic.Transport.Serializer serializer, string json) where T : class
    {
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));
        var result = serializer.Deserialize<T>(stream);
        Assert.NotNull(result);
        return result;
    }

    private sealed class TestConfiguration<T> : ElasticConfiguration where T : class
    {
        private readonly Action<JsonSerializerOptions>? _configure;

        public TestConfiguration(Action<JsonSerializerOptions>? configure = null)
        {
            _configure = configure;
            AddIndex(new Index<T>(this));
        }

        protected override void ConfigureSourceSerializerOptions(JsonSerializerOptions options)
        {
            _configure?.Invoke(options);
            base.ConfigureSourceSerializerOptions(options);
        }
    }

    private sealed class PublicVersionDocument : IIdentity, IVersioned
    {
        public string Id { get; set; } = null!;
        public string? Version { get; set; }
        public PublicVersionDocument? Child { get; set; }
    }

    private sealed class ExplicitVersionDocument : IVersioned
    {
        public string? Version { get; set; }
        public ExplicitVersionDocument? Child { get; set; }
        string? IVersioned.Version { get; set; }
    }

    private sealed class IgnoredVersionDocument : IVersioned
    {
        [JsonIgnore]
        public string? Version { get; set; }
        public string? Name { get; set; }
    }

    private class PopulateDocument : IVersioned
    {
        string? IVersioned.Version { get; set; }
        public virtual List<PublicVersionDocument> Children { get; set; } = [new() { Id = "existing", Version = "business-1" }];
    }

    private sealed class PropertyPopulateDocument : PopulateDocument
    {
        [JsonObjectCreationHandling(JsonObjectCreationHandling.Populate)]
        public override List<PublicVersionDocument> Children { get; set; } = [new() { Id = "existing", Version = "business-1" }];
    }

    [JsonObjectCreationHandling(JsonObjectCreationHandling.Populate)]
    private sealed class TypePopulateDocument : PopulateDocument
    {
    }

    [JsonObjectCreationHandling(JsonObjectCreationHandling.Populate)]
    private sealed class ReplacePopulateDocument : PopulateDocument
    {
        [JsonObjectCreationHandling(JsonObjectCreationHandling.Replace)]
        public override List<PublicVersionDocument> Children { get; set; } = [new() { Id = "existing", Version = "business-1" }];
    }

    private class ReferenceDocument : IVersioned
    {
        public string? Name { get; set; }
        public ReferenceDocument? First { get; set; }
        public ReferenceDocument? Second { get; set; }
        public ReferenceDocument? Parent { get; set; }
        string? IVersioned.Version { get; set; }
    }

    private sealed class NonVersionedDocument
    {
        public ReferenceDocument? First { get; set; }
        public ReferenceDocument? Second { get; set; }
    }
}
