using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Serialization;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class DocumentVersionSourceExclusionTests
{
    private static JsonSerializerOptions CreateOptions(params System.Type[] documentTypes)
    {
        return new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase }
            .ConfigureFoundatioRepositoryDefaults()
            .ExcludeDocumentVersionFromSource(documentTypes);
    }

    [Fact]
    public void Serialize_WithRegisteredRootDocument_ExcludesRootVersionAndKeepsNestedVersion()
    {
        // Arrange
        var options = CreateOptions(typeof(VersionedRootDocument));
        var document = new VersionedRootDocument
        {
            Id = "1",
            Version = "1:5",
            MetaData = new VersionedMetaData { Version = "business-7", Name = "nested" }
        };

        // Act
        string json = JsonSerializer.Serialize(document, options);

        // Assert
        using var parsed = JsonDocument.Parse(json);
        Assert.False(parsed.RootElement.TryGetProperty("version", out _));
        Assert.Equal("business-7", parsed.RootElement.GetProperty("metaData").GetProperty("version").GetString());
    }

    [Fact]
    public void Deserialize_WithStaleRootVersionInSource_IgnoresRootVersion()
    {
        // Arrange
        var options = CreateOptions(typeof(VersionedRootDocument));
        const string json = """{"id":"1","version":"112:2","metaData":{"version":"business-7","name":"nested"}}""";

        // Act
        var document = JsonSerializer.Deserialize<VersionedRootDocument>(json, options);

        // Assert
        Assert.NotNull(document);
        Assert.Null(document.Version);
        Assert.Equal("business-7", document.MetaData?.Version);
    }

    [Fact]
    public void Serialize_WithUnregisteredVersionedType_KeepsVersion()
    {
        // Arrange
        var options = CreateOptions(typeof(VersionedRootDocument));

        // Act
        string json = JsonSerializer.Serialize(new VersionedMetaData { Version = "3", Name = "standalone" }, options);

        // Assert
        Assert.Contains("\"version\":\"3\"", json);
    }

    [Fact]
    public void Serialize_WithDerivedDocumentType_ExcludesVersion()
    {
        // Arrange
        var options = CreateOptions(typeof(VersionedRootDocument));

        // Act
        string derivedJson = JsonSerializer.Serialize(new DerivedRootDocument { Id = "1", Version = "1:5", Extra = "x" }, options);
        string polymorphicJson = JsonSerializer.Serialize<VersionedRootDocument>(new DerivedRootDocument { Id = "1", Version = "1:5", Extra = "x" }, options);
        var deserialized = JsonSerializer.Deserialize<DerivedRootDocument>("""{"id":"1","version":"9:9","extra":"x"}""", options);

        // Assert
        Assert.DoesNotContain("version", derivedJson);
        Assert.Contains("\"extra\":\"x\"", derivedJson);
        Assert.DoesNotContain("version", polymorphicJson);
        Assert.NotNull(deserialized);
        Assert.Null(deserialized.Version);
    }

    [Fact]
    public void Serialize_WithRootTypeNestedInAnotherDocument_KeepsNestedVersion()
    {
        // Arrange
        var options = CreateOptions(typeof(VersionedRootDocument), typeof(ContainerDocument));
        var container = new ContainerDocument
        {
            Id = "c",
            Version = "1:1",
            Child = new VersionedRootDocument { Id = "child", Version = "2:7" },
            Children = [new VersionedRootDocument { Id = "child2", Version = "3:8" }]
        };

        // Act
        string json = JsonSerializer.Serialize(container, options);
        var roundTripped = JsonSerializer.Deserialize<ContainerDocument>(json, options);

        // Assert
        using var parsed = JsonDocument.Parse(json);
        Assert.False(parsed.RootElement.TryGetProperty("version", out _));
        Assert.Equal("2:7", parsed.RootElement.GetProperty("child").GetProperty("version").GetString());
        Assert.Equal("3:8", parsed.RootElement.GetProperty("children")[0].GetProperty("version").GetString());
        Assert.Equal("2:7", roundTripped?.Child?.Version);
        Assert.Equal("3:8", roundTripped?.Children[0].Version);
    }

    [Fact]
    public void Serialize_WithRootTypeInObjectAndDictionaryProperties_KeepsNestedVersion()
    {
        // Arrange
        var options = CreateOptions(typeof(VersionedRootDocument), typeof(DynamicContainerDocument));
        var container = new DynamicContainerDocument
        {
            Id = "c",
            Version = "1:1",
            Payload = new VersionedRootDocument { Id = "payload", Version = "2:7" },
            Items = new Dictionary<string, object> { ["entry"] = new VersionedRootDocument { Id = "entry", Version = "3:8" } },
            List = [new VersionedRootDocument { Id = "listed", Version = "4:9" }]
        };

        // Act
        string json = JsonSerializer.Serialize(container, options);

        // Assert
        using var parsed = JsonDocument.Parse(json);
        Assert.False(parsed.RootElement.TryGetProperty("version", out _));
        Assert.Equal("2:7", parsed.RootElement.GetProperty("payload").GetProperty("version").GetString());
        Assert.Equal("3:8", parsed.RootElement.GetProperty("items").GetProperty("entry").GetProperty("version").GetString());
        Assert.Equal("4:9", parsed.RootElement.GetProperty("list")[0].GetProperty("version").GetString());
    }

    [Fact]
    public void Serialize_WithObjectPropertyHoldingNonDocumentValues_SerializesNormally()
    {
        // Arrange
        var options = CreateOptions(typeof(VersionedRootDocument), typeof(DynamicContainerDocument));
        var container = new DynamicContainerDocument
        {
            Id = "c",
            Payload = "text",
            Items = new Dictionary<string, object> { ["number"] = 5, ["nested"] = new VersionedMetaData { Version = "business-7", Name = "n" } }
        };

        // Act
        string json = JsonSerializer.Serialize(container, options);

        // Assert
        using var parsed = JsonDocument.Parse(json);
        Assert.Equal("text", parsed.RootElement.GetProperty("payload").GetString());
        Assert.Equal(5, parsed.RootElement.GetProperty("items").GetProperty("number").GetInt32());
        Assert.Equal("business-7", parsed.RootElement.GetProperty("items").GetProperty("nested").GetProperty("version").GetString());
    }

    [Fact]
    public void Serialize_WithRegisteredDocumentBehindPolymorphicBase_KeepsNestedVersion()
    {
        var options = CreateOptions(typeof(PolymorphicContainerDocument), typeof(PolymorphicRootDocument));
        var document = new PolymorphicContainerDocument
        {
            Version = "1:5",
            Child = new PolymorphicRootDocument { Version = "business-2", Name = "nested" }
        };

        string json = JsonSerializer.Serialize(document, options);
        var roundTripped = JsonSerializer.Deserialize<PolymorphicContainerDocument>(json, options);

        using var parsed = JsonDocument.Parse(json);
        Assert.False(parsed.RootElement.TryGetProperty("version", out _));
        Assert.Equal("business-2", parsed.RootElement.GetProperty("child").GetProperty("version").GetString());
        Assert.Equal("business-2", Assert.IsType<PolymorphicRootDocument>(roundTripped?.Child).Version);
    }

    [Fact]
    public void Serialize_WithRegisteredDocumentInNonGenericCollection_KeepsNestedVersion()
    {
        var options = CreateOptions(typeof(CollectionContainerDocument), typeof(VersionedRootDocument));
        var document = new CollectionContainerDocument
        {
            Version = "1:5",
            Children = [new VersionedRootDocument { Id = "nested", Version = "business-2" }]
        };

        string json = JsonSerializer.Serialize(document, options);
        var roundTripped = JsonSerializer.Deserialize<CollectionContainerDocument>(json, options);

        using var parsed = JsonDocument.Parse(json);
        Assert.False(parsed.RootElement.TryGetProperty("version", out _));
        Assert.Equal("business-2", parsed.RootElement.GetProperty("children")[0].GetProperty("version").GetString());
        Assert.Equal("business-2", roundTripped?.Children[0].Version);
    }

    [Fact]
    public void Serialize_WithExtensionData_RoundTripsValuesAndExcludesRootVersion()
    {
        var options = CreateOptions(typeof(ExtensionDataDocument));
        var document = new ExtensionDataDocument
        {
            Id = "extension",
            Version = "1:5",
            Extra = new Dictionary<string, object> { ["label"] = "preserved", ["number"] = 7 }
        };

        string json = JsonSerializer.Serialize(document, options);
        var roundTripped = JsonSerializer.Deserialize<ExtensionDataDocument>(json, options);

        using var parsed = JsonDocument.Parse(json);
        Assert.False(parsed.RootElement.TryGetProperty("version", out _));
        Assert.Equal("preserved", parsed.RootElement.GetProperty("label").GetString());
        Assert.Equal(7, parsed.RootElement.GetProperty("number").GetInt32());
        Assert.NotNull(roundTripped);
        Assert.Null(roundTripped.Version);
        Assert.Equal(2, roundTripped.Extra.Count);
    }

    [Fact]
    public void Serialize_WithRegisteredDocumentInExtensionData_KeepsNestedVersion()
    {
        var options = CreateOptions(typeof(ExtensionDataDocument), typeof(VersionedRootDocument));
        var document = new ExtensionDataDocument
        {
            Id = "extension",
            Version = "1:5",
            Extra = new Dictionary<string, object>
            {
                ["child"] = new VersionedRootDocument { Id = "nested", Version = "business-2" }
            }
        };

        string json = JsonSerializer.Serialize(document, options);

        using var parsed = JsonDocument.Parse(json);
        Assert.False(parsed.RootElement.TryGetProperty("version", out _));
        Assert.Equal("business-2", parsed.RootElement.GetProperty("child").GetProperty("version").GetString());
    }

    [Theory]
    [InlineData("\"1:5\"")]
    [InlineData("23")]
    [InlineData("{\"legacy\":true}")]
    public void Deserialize_WithStaleRootVersionAndExtensionData_DoesNotCaptureVersion(string storedVersion)
    {
        var options = CreateOptions(typeof(ExtensionDataDocument));

        var document = JsonSerializer.Deserialize<ExtensionDataDocument>($$"""{"id":"extension","version":{{storedVersion}},"label":"preserved"}""", options);

        Assert.NotNull(document);
        Assert.Null(document.Version);
        Assert.False(document.Extra.ContainsKey("version"));
        Assert.Single(document.Extra);
        Assert.DoesNotContain("\"version\"", JsonSerializer.Serialize(document, options));
    }

    [Fact]
    public void Serialize_WithCustomExtensionDataDictionary_PreservesValuesWithoutMutation()
    {
        var options = CreateOptions(typeof(CustomExtensionDataDocument), typeof(VersionedRootDocument));
        var child = new VersionedRootDocument { Id = "nested", Version = "business-2" };
        var extra = new ExtensionDictionary { ["child"] = child, ["name"] = "preserved", ["empty"] = null! };
        var document = new CustomExtensionDataDocument { Version = "1:5", Extra = extra };

        string json = JsonSerializer.Serialize(document, options);
        var roundTripped = JsonSerializer.Deserialize<CustomExtensionDataDocument>(json, options);

        using var parsed = JsonDocument.Parse(json);
        Assert.False(parsed.RootElement.TryGetProperty("version", out _));
        Assert.Equal("business-2", parsed.RootElement.GetProperty("child").GetProperty("version").GetString());
        Assert.Equal("preserved", parsed.RootElement.GetProperty("name").GetString());
        Assert.Equal(JsonValueKind.Null, parsed.RootElement.GetProperty("empty").ValueKind);
        Assert.Same(extra, document.Extra);
        Assert.Same(child, extra["child"]);
        Assert.Equal("business-2", child.Version);
        Assert.NotNull(roundTripped);
        Assert.IsType<ExtensionDictionary>(roundTripped.Extra);
        Assert.Equal(3, roundTripped.Extra.Count);
    }

    [Fact]
    public void Serialize_WithNullExtensionDataDictionary_SerializesNormally()
    {
        var options = CreateOptions(typeof(ExtensionDataDocument));
        var document = new ExtensionDataDocument { Id = "extension", Version = "1:5", Extra = null! };

        string json = JsonSerializer.Serialize(document, options);

        using var parsed = JsonDocument.Parse(json);
        Assert.False(parsed.RootElement.TryGetProperty("version", out _));
        Assert.Equal("extension", parsed.RootElement.GetProperty("id").GetString());
        Assert.Single(parsed.RootElement.EnumerateObject());
    }

    [Fact]
    public void Serialize_WithJsonElementExtensionData_KeepsValuesAndIgnoresLegacyVersion()
    {
        var options = CreateOptions(typeof(JsonElementExtensionDataDocument));
        var document = JsonSerializer.Deserialize<JsonElementExtensionDataDocument>("""{"version":"1:5","child":{"version":"business-2"}}""", options);

        Assert.NotNull(document);
        string json = JsonSerializer.Serialize(document, options);

        using var parsed = JsonDocument.Parse(json);
        Assert.Null(document.Version);
        Assert.False(document.Extra.ContainsKey("version"));
        Assert.False(parsed.RootElement.TryGetProperty("version", out _));
        Assert.Equal("business-2", parsed.RootElement.GetProperty("child").GetProperty("version").GetString());
    }

    [Fact]
    public void Serialize_WithExplicitConcurrencyVersion_KeepsPublicBusinessVersion()
    {
        var options = CreateOptions(typeof(ExplicitVersionDocument));
        var document = new ExplicitVersionDocument { Id = "explicit", Version = "business-7" };
        ((IVersioned)document).Version = "1:5";

        string json = JsonSerializer.Serialize(document, options);
        var roundTripped = JsonSerializer.Deserialize<ExplicitVersionDocument>(json, options);

        using var parsed = JsonDocument.Parse(json);
        Assert.Equal("business-7", parsed.RootElement.GetProperty("version").GetString());
        Assert.NotNull(roundTripped);
        Assert.Equal("business-7", roundTripped.Version);
        Assert.Null(((IVersioned)roundTripped).Version);
    }

    [Fact]
    public void ExcludeDocumentVersionFromSource_WithConcurrentFirstUse_IsThreadSafe()
    {
        // Arrange
        var options = CreateOptions(typeof(VersionedRootDocument), typeof(ContainerDocument));
        var container = new ContainerDocument { Id = "c", Version = "1:1", Child = new VersionedRootDocument { Id = "child", Version = "2:7" } };

        // Act
        var results = new System.Collections.Concurrent.ConcurrentBag<string>();
        System.Threading.Tasks.Parallel.For(0, 32, _ => results.Add(JsonSerializer.Serialize(container, options)));

        // Assert
        Assert.All(results, json => Assert.Contains("\"version\":\"2:7\"", json));
    }

    [Fact]
    public void ExcludeDocumentVersionFromSource_WithSeparateOptions_DoesNotAffectOtherOptions()
    {
        // Arrange
        var registered = CreateOptions(typeof(VersionedRootDocument));
        var other = CreateOptions(typeof(ContainerDocument));
        var document = new VersionedRootDocument { Id = "1", Version = "1:5" };

        // Act & Assert
        Assert.DoesNotContain("version", JsonSerializer.Serialize(document, registered));
        Assert.Contains("\"version\":\"1:5\"", JsonSerializer.Serialize(document, other));
    }

    private class VersionedRootDocument : IIdentity, IVersioned
    {
        public string Id { get; set; } = null!;
        public string? Version { get; set; }
        public VersionedMetaData? MetaData { get; set; }
    }

    private sealed class DerivedRootDocument : VersionedRootDocument
    {
        public string? Extra { get; set; }
    }

    private sealed class ContainerDocument : IIdentity, IVersioned
    {
        public string Id { get; set; } = null!;
        public string? Version { get; set; }
        public VersionedRootDocument? Child { get; set; }
        public List<VersionedRootDocument> Children { get; set; } = [];
    }

    private sealed class DynamicContainerDocument : IIdentity, IVersioned
    {
        public string Id { get; set; } = null!;
        public string? Version { get; set; }
        public object? Payload { get; set; }
        public Dictionary<string, object> Items { get; set; } = [];
        public List<object> List { get; set; } = [];
    }

    [JsonDerivedType(typeof(PolymorphicRootDocument), "root")]
    private abstract class PolymorphicBase
    {
        public string? Name { get; set; }
    }

    private sealed class PolymorphicRootDocument : PolymorphicBase, IVersioned
    {
        public string? Version { get; set; }
    }

    private sealed class PolymorphicContainerDocument : IVersioned
    {
        public string? Version { get; set; }
        public PolymorphicBase? Child { get; set; }
    }

    private sealed class DocumentList : List<VersionedRootDocument>
    {
    }

    private sealed class CollectionContainerDocument : IVersioned
    {
        public string? Version { get; set; }
        public DocumentList Children { get; set; } = [];
    }

    private sealed class VersionedMetaData : IVersioned
    {
        public string? Version { get; set; }
        public string? Name { get; set; }
    }

    private sealed class ExtensionDataDocument : IIdentity, IVersioned
    {
        public string Id { get; set; } = null!;
        public string? Version { get; set; }

        [JsonExtensionData]
        public Dictionary<string, object> Extra { get; set; } = [];
    }

    private sealed class ExplicitVersionDocument : IIdentity, IVersioned
    {
        public string Id { get; set; } = null!;
        public string? Version { get; set; }
        string? IVersioned.Version { get; set; }
    }

    private sealed class CustomExtensionDataDocument : IVersioned
    {
        public string? Version { get; set; }

        [JsonExtensionData]
        public ExtensionDictionary Extra { get; set; } = [];
    }

    private sealed class ExtensionDictionary : Dictionary<string, object>
    {
    }

    private sealed class JsonElementExtensionDataDocument : IVersioned
    {
        public string? Version { get; set; }

        [JsonExtensionData]
        public Dictionary<string, JsonElement> Extra { get; set; } = [];
    }
}
