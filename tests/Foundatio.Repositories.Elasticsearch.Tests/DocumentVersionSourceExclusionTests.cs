using System.Text.Json;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Serialization;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class DocumentVersionSourceExclusionTests
{
    [Fact]
    public void Serialize_WithRegisteredRootDocument_ExcludesRootVersionAndKeepsNestedVersion()
    {
        // Arrange
        DocumentVersionSourceExclusion.RegisterDocumentType(typeof(VersionedRootDocument));
        var options = new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase }
            .ConfigureFoundatioRepositoryDefaults()
            .ExcludeDocumentVersionFromSource();
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
        DocumentVersionSourceExclusion.RegisterDocumentType(typeof(VersionedRootDocument));
        var options = new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase }
            .ConfigureFoundatioRepositoryDefaults()
            .ExcludeDocumentVersionFromSource();
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
        var options = new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase }.ExcludeDocumentVersionFromSource();

        // Act
        string json = JsonSerializer.Serialize(new VersionedMetaData { Version = "3", Name = "standalone" }, options);

        // Assert
        Assert.Contains("\"version\":\"3\"", json);
    }

    private sealed class VersionedRootDocument : IIdentity, IVersioned
    {
        public string Id { get; set; } = null!;
        public string Version { get; set; } = null!;
        public VersionedMetaData? MetaData { get; set; }
    }

    private sealed class VersionedMetaData : IVersioned
    {
        public string Version { get; set; } = null!;
        public string? Name { get; set; }
    }
}
