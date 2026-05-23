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
    public void Modify_EmptyArray_OmitsProperty()
    {
        // Arrange
        var obj = new ModelWithArray { Name = "test", Values = [] };

        // Act
        string json = JsonSerializer.Serialize(obj, _options);

        // Assert
        Assert.Equal("{\"Name\":\"test\"}", json);
    }

    [Fact]
    public void Modify_EmptyDictionary_OmitsProperty()
    {
        // Arrange
        var obj = new ModelWithDictionary { Name = "test", Tags = new Dictionary<string, string>() };

        // Act
        string json = JsonSerializer.Serialize(obj, _options);

        // Assert
        Assert.Equal("{\"Name\":\"test\"}", json);
    }

    [Fact]
    public void Modify_EmptyList_OmitsProperty()
    {
        // Arrange
        var obj = new ModelWithList { Name = "test", Items = [] };

        // Act
        string json = JsonSerializer.Serialize(obj, _options);

        // Assert
        Assert.Equal("{\"Name\":\"test\"}", json);
    }

    [Fact]
    public void Modify_EmptyReadOnlyCollection_OmitsProperty()
    {
        // Arrange
        var obj = new ModelWithReadOnlyCollection { Name = "test", Items = new List<string>().AsReadOnly() };

        // Act
        string json = JsonSerializer.Serialize(obj, _options);

        // Assert
        Assert.Equal("{\"Name\":\"test\"}", json);
    }

    [Fact]
    public void Modify_ScalarProperties_PreservesAll()
    {
        // Arrange
        var obj = new ModelWithScalars { Name = "test", Count = 0, Active = false };

        // Act
        string json = JsonSerializer.Serialize(obj, _options);

        // Assert
        Assert.Equal("{\"Name\":\"test\",\"Count\":0,\"Active\":false}", json);
    }

    [Fact]
    public void Modify_NullCollection_OmitsProperty()
    {
        // Arrange
        var obj = new ModelWithList { Name = "test", Items = null! };

        // Act
        string json = JsonSerializer.Serialize(obj, _options);

        // Assert
        Assert.Equal("{\"Name\":\"test\"}", json);
    }

    [Fact]
    public void Modify_PopulatedArray_IncludesProperty()
    {
        // Arrange
        var obj = new ModelWithArray { Name = "test", Values = [1, 2, 3] };

        // Act
        string json = JsonSerializer.Serialize(obj, _options);

        // Assert
        Assert.Equal("{\"Name\":\"test\",\"Values\":[1,2,3]}", json);
    }

    [Fact]
    public void Modify_PopulatedDictionary_IncludesProperty()
    {
        // Arrange
        var obj = new ModelWithDictionary { Name = "test", Tags = new Dictionary<string, string> { ["key"] = "val" } };

        // Act
        string json = JsonSerializer.Serialize(obj, _options);

        // Assert
        Assert.Equal("{\"Name\":\"test\",\"Tags\":{\"key\":\"val\"}}", json);
    }

    [Fact]
    public void Modify_PopulatedList_IncludesProperty()
    {
        // Arrange
        var obj = new ModelWithList { Name = "test", Items = ["a", "b"] };

        // Act
        string json = JsonSerializer.Serialize(obj, _options);

        // Assert
        Assert.Equal("{\"Name\":\"test\",\"Items\":[\"a\",\"b\"]}", json);
    }

    [Fact]
    public void Modify_PopulatedReadOnlyCollection_IncludesProperty()
    {
        // Arrange
        var obj = new ModelWithReadOnlyCollection { Name = "test", Items = new List<string> { "x", "y" }.AsReadOnly() };

        // Act
        string json = JsonSerializer.Serialize(obj, _options);

        // Assert
        Assert.Equal("{\"Name\":\"test\",\"Items\":[\"x\",\"y\"]}", json);
    }

    [Fact]
    public void Modify_EmptyIEnumerableProperty_OmitsProperty()
    {
        // Arrange
        var obj = new ModelWithEnumerable { Name = "test", Items = [] };

        // Act
        string json = JsonSerializer.Serialize(obj, _options);

        // Assert
        Assert.Equal("{\"Name\":\"test\"}", json);
    }

    [Fact]
    public void Modify_PopulatedIEnumerableProperty_IncludesProperty()
    {
        // Arrange
        var obj = new ModelWithEnumerable { Name = "test", Items = ["a"] };

        // Act
        string json = JsonSerializer.Serialize(obj, _options);

        // Assert
        Assert.Equal("{\"Name\":\"test\",\"Items\":[\"a\"]}", json);
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

    private class ModelWithReadOnlyCollection
    {
        public string Name { get; set; } = null!;
        public IReadOnlyCollection<string> Items { get; set; } = null!;
    }

    private class ModelWithScalars
    {
        public string Name { get; set; } = null!;
        public int Count { get; set; }
        public bool Active { get; set; }
    }

    private class ModelWithEnumerable
    {
        public string Name { get; set; } = null!;
        public IEnumerable<string> Items { get; set; } = null!;
    }
}
