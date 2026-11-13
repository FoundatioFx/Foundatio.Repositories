using System.Collections.Generic;
using System.Text.Json;
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

    private sealed class VersionedMetaData : IVersioned
    {
        public string? Version { get; set; }
        public string? Name { get; set; }
    }
}
