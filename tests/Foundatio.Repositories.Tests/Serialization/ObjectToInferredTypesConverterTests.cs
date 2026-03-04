using System;
using System.Collections.Generic;
using System.Text.Json;
using Foundatio.Repositories.Serialization;
using Foundatio.Serializer;
using Xunit;

namespace Foundatio.Repositories.Tests.Serialization;

public class ObjectToInferredTypesConverterTests
{
    private static readonly ITextSerializer _serializer = new SystemTextJsonSerializer(
        new JsonSerializerOptions().ConfigureFoundatioRepositoryDefaults());

    [Fact]
    public void Read_WithInteger_ReturnsLong()
    {
        // Arrange
        string json = "42";

        // Act
        var result = _serializer.Deserialize<object>(json);

        // Assert
        Assert.IsType<long>(result);
        Assert.Equal(42L, result);
    }

    [Fact]
    public void Read_WithNegativeInteger_ReturnsLong()
    {
        // Arrange
        string json = "-100";

        // Act
        var result = _serializer.Deserialize<object>(json);

        // Assert
        Assert.IsType<long>(result);
        Assert.Equal(-100L, result);
    }

    [Fact]
    public void Read_WithZeroInteger_ReturnsLong()
    {
        // Arrange
        string json = "0";

        // Act
        var result = _serializer.Deserialize<object>(json);

        // Assert
        Assert.IsType<long>(result);
        Assert.Equal(0L, result);
    }

    [Fact]
    public void Read_WithFloatingPoint_ReturnsDouble()
    {
        // Arrange
        string json = "42.5";

        // Act
        var result = _serializer.Deserialize<object>(json);

        // Assert
        Assert.IsType<double>(result);
        Assert.Equal(42.5, result);
    }

    [Fact]
    public void Read_WithZeroPointZero_ReturnsDouble()
    {
        // Arrange
        string json = "0.0";

        // Act
        var result = _serializer.Deserialize<object>(json);

        // Assert
        Assert.IsType<double>(result);
        Assert.Equal(0.0, result);
    }

    [Fact]
    public void Read_WithScientificNotation_ReturnsDouble()
    {
        // Arrange
        string json = "1.5e3";

        // Act
        var result = _serializer.Deserialize<object>(json);

        // Assert
        Assert.IsType<double>(result);
        Assert.Equal(1500.0, result);
    }

    [Fact]
    public void Read_WithString_ReturnsString()
    {
        // Arrange
        string json = "\"hello\"";

        // Act
        var result = _serializer.Deserialize<object>(json);

        // Assert
        Assert.IsType<string>(result);
        Assert.Equal("hello", result);
    }

    [Fact]
    public void Read_WithTrue_ReturnsBoolTrue()
    {
        // Arrange
        string json = "true";

        // Act
        var result = _serializer.Deserialize<object>(json);

        // Assert
        Assert.IsType<bool>(result);
        Assert.True((bool)result);
    }

    [Fact]
    public void Read_WithFalse_ReturnsBoolFalse()
    {
        // Arrange
        string json = "false";

        // Act
        var result = _serializer.Deserialize<object>(json);

        // Assert
        Assert.IsType<bool>(result);
        Assert.False((bool)result);
    }

    [Fact]
    public void Read_WithNull_ReturnsNull()
    {
        // Arrange
        string json = "null";

        // Act
        var result = _serializer.Deserialize<object>(json);

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public void Read_WithIso8601Date_ReturnsDateTimeOffset()
    {
        // Arrange
        string json = "\"2026-03-03T12:00:00Z\"";

        // Act
        var result = _serializer.Deserialize<object>(json);

        // Assert
        Assert.IsType<DateTimeOffset>(result);
        Assert.Equal(new DateTimeOffset(2026, 3, 3, 12, 0, 0, TimeSpan.Zero), result);
    }

    [Fact]
    public void Read_WithJsonObject_ReturnsCaseInsensitiveDictionary()
    {
        // Arrange
        string json = "{\"Name\":\"test\",\"Count\":42}";

        // Act
        var result = _serializer.Deserialize<object>(json);

        // Assert
        var dict = Assert.IsType<Dictionary<string, object>>(result);
        Assert.Equal("test", dict["name"]);
        Assert.Equal("test", dict["NAME"]);
        Assert.Equal(42L, dict["count"]);
    }

    [Fact]
    public void Read_WithJsonArray_ReturnsList()
    {
        // Arrange
        string json = "[1,2,3]";

        // Act
        var result = _serializer.Deserialize<object>(json);

        // Assert
        var list = Assert.IsType<List<object>>(result);
        Assert.Equal(3, list.Count);
        Assert.Equal(1L, list[0]);
        Assert.Equal(2L, list[1]);
        Assert.Equal(3L, list[2]);
    }

    [Fact]
    public void Read_WithMixedArray_PreservesElementTypes()
    {
        // Arrange
        string json = "[\"hello\",42,true,null,1.5]";

        // Act
        var result = _serializer.Deserialize<object>(json);

        // Assert
        var list = Assert.IsType<List<object>>(result);
        Assert.Equal(5, list.Count);
        Assert.IsType<string>(list[0]);
        Assert.IsType<long>(list[1]);
        Assert.IsType<bool>(list[2]);
        Assert.Null(list[3]);
        Assert.IsType<double>(list[4]);
    }

    [Fact]
    public void Read_WithNestedObject_PreservesStructure()
    {
        // Arrange
        string json = "{\"outer\":{\"inner\":\"value\"}}";

        // Act
        var result = _serializer.Deserialize<object>(json);

        // Assert
        var dict = Assert.IsType<Dictionary<string, object>>(result);
        var inner = Assert.IsType<Dictionary<string, object>>(dict["outer"]);
        Assert.Equal("value", inner["inner"]);
    }

    [Fact]
    public void RoundTrip_WithObjectArray_PreservesTypesAndValues()
    {
        // Arrange
        var original = new object[] { "test", 42L, true, 1.5 };

        // Act
        string json = _serializer.SerializeToString(original);
        var result = _serializer.Deserialize<object[]>(json);

        // Assert
        Assert.Equal(4, result.Length);
        Assert.Equal("test", result[0]);
        Assert.Equal(42L, result[1]);
        Assert.Equal(true, result[2]);
        Assert.Equal(1.5, result[3]);
    }

    [Fact]
    public void Write_WithNull_WritesNullLiteral()
    {
        // Arrange
        object value = null;

        // Act
        string json = _serializer.SerializeToString(value);

        // Assert
        Assert.Equal("null", json);
    }

    [Fact]
    public void Write_WithLong_WritesNumber()
    {
        // Arrange
        object value = 42L;

        // Act
        string json = _serializer.SerializeToString(value);

        // Assert
        Assert.Equal("42", json);
    }

    [Fact]
    public void Write_WithString_WritesQuotedString()
    {
        // Arrange
        object value = "hello";

        // Act
        string json = _serializer.SerializeToString(value);

        // Assert
        Assert.Equal("\"hello\"", json);
    }
}
