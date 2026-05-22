using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Foundatio.Repositories.Serialization;
using Xunit;

namespace Foundatio.Repositories.Tests.Serialization;

public class EmptyCollectionModifierTests
{
    private static readonly JsonSerializerOptions _options = new()
    {
        TypeInfoResolver = new DefaultJsonTypeInfoResolver
        {
            Modifiers = { EmptyCollectionModifier.Modify }
        }
    };

    [Fact]
    public void Modify_WithEmptyArray_OmitsProperty()
    {
        // Arrange
        var obj = new ModelWithArray { Name = "test", Values = [] };

        // Act
        string json = JsonSerializer.Serialize(obj, _options);

        // Assert
        Assert.Contains("\"Name\"", json);
        Assert.DoesNotContain("Values", json);
    }

    [Fact]
    public void Modify_WithEmptyDictionary_OmitsProperty()
    {
        // Arrange
        var obj = new ModelWithDictionary { Name = "test", Tags = new Dictionary<string, string>() };

        // Act
        string json = JsonSerializer.Serialize(obj, _options);

        // Assert
        Assert.Contains("\"Name\"", json);
        Assert.DoesNotContain("Tags", json);
    }

    [Fact]
    public void Modify_WithEmptyList_OmitsProperty()
    {
        // Arrange
        var obj = new ModelWithList { Name = "test", Items = [] };

        // Act
        string json = JsonSerializer.Serialize(obj, _options);

        // Assert
        Assert.Contains("\"Name\"", json);
        Assert.DoesNotContain("Items", json);
    }

    [Fact]
    public void Modify_WithNonCollectionProperties_PreservesAll()
    {
        // Arrange
        var obj = new ModelWithScalars { Name = "test", Count = 0, Active = false };

        // Act
        string json = JsonSerializer.Serialize(obj, _options);

        // Assert
        Assert.Contains("\"Name\"", json);
        Assert.Contains("\"Count\"", json);
        Assert.Contains("\"Active\"", json);
    }

    [Fact]
    public void Modify_WithNullCollection_OmitsProperty()
    {
        // Arrange
        var obj = new ModelWithList { Name = "test", Items = null! };

        // Act
        string json = JsonSerializer.Serialize(obj, _options);

        // Assert
        Assert.Contains("\"Name\"", json);
        Assert.DoesNotContain("Items", json);
    }

    [Fact]
    public void Modify_WithPopulatedArray_IncludesProperty()
    {
        // Arrange
        var obj = new ModelWithArray { Name = "test", Values = [1, 2, 3] };

        // Act
        string json = JsonSerializer.Serialize(obj, _options);

        // Assert
        Assert.Contains("\"Values\"", json);
    }

    [Fact]
    public void Modify_WithPopulatedDictionary_IncludesProperty()
    {
        // Arrange
        var obj = new ModelWithDictionary { Name = "test", Tags = new Dictionary<string, string> { ["key"] = "val" } };

        // Act
        string json = JsonSerializer.Serialize(obj, _options);

        // Assert
        Assert.Contains("\"Tags\"", json);
        Assert.Contains("\"key\"", json);
    }

    [Fact]
    public void Modify_WithPopulatedList_IncludesProperty()
    {
        // Arrange
        var obj = new ModelWithList { Name = "test", Items = ["a", "b"] };

        // Act
        string json = JsonSerializer.Serialize(obj, _options);

        // Assert
        Assert.Contains("\"Items\"", json);
        Assert.Contains("\"a\"", json);
    }

    private class ModelWithArray
    {
        public string Name { get; set; } = null!;
        public int[] Values { get; set; } = null!;
    }

    private class ModelWithDictionary
    {
        public string Name { get; set; } = null!;
        public Dictionary<string, string> Tags { get; set; } = null!;
    }

    private class ModelWithList
    {
        public string Name { get; set; } = null!;
        public List<string> Items { get; set; } = null!;
    }

    private class ModelWithScalars
    {
        public string Name { get; set; } = null!;
        public int Count { get; set; }
        public bool Active { get; set; }
    }
}
