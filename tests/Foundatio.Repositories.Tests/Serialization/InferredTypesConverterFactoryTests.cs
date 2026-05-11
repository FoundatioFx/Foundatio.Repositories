using System;
using System.Text.Json;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Serialization;
using Foundatio.Serializer;
using Xunit;

namespace Foundatio.Repositories.Tests.Serialization;

public class InferredTypesConverterFactoryTests
{
    private static readonly ITextSerializer _serializer = new SystemTextJsonSerializer(
        new JsonSerializerOptions().ConfigureFoundatioRepositoryDefaults());

    [Fact]
    public void Read_WithDateOnlyString_ReturnsString()
    {
        // Arrange
        var json = """{"key": "2025-01-01", "keyAsString": "2025-01-01"}""";

        // Act
        var result = _serializer.Deserialize<KeyedBucket<object>>(json);

        // Assert
        Assert.NotNull(result);
        Assert.IsType<string>(result.Key);
        Assert.Equal("2025-01-01", result.Key);
    }

    [Fact]
    public void Read_WithIsoDateTimeString_ReturnsDateTimeOffset()
    {
        // Arrange
        var json = """{"key": "2025-01-01T10:30:00Z", "keyAsString": "2025-01-01T10:30:00Z"}""";

        // Act
        var result = _serializer.Deserialize<KeyedBucket<object>>(json);

        // Assert
        Assert.NotNull(result);
        Assert.IsType<DateTimeOffset>(result.Key);
    }

    [Fact]
    public void Read_WithPlainString_ReturnsString()
    {
        // Arrange
        var json = """{"key": "hello-world", "keyAsString": "hello-world"}""";

        // Act
        var result = _serializer.Deserialize<KeyedBucket<object>>(json);

        // Assert
        Assert.NotNull(result);
        Assert.IsType<string>(result.Key);
        Assert.Equal("hello-world", result.Key);
    }

    [Fact]
    public void Read_WithNumericString_ReturnsString()
    {
        // Arrange
        var json = """{"key": "12345", "keyAsString": "12345"}""";

        // Act
        var result = _serializer.Deserialize<KeyedBucket<object>>(json);

        // Assert
        Assert.NotNull(result);
        Assert.IsType<string>(result.Key);
        Assert.Equal("12345", result.Key);
    }

    [Fact]
    public void Read_WithLongValue_ReturnsLong()
    {
        // Arrange
        var json = """{"key": 9007199254740993, "keyAsString": "9007199254740993"}""";

        // Act
        var result = _serializer.Deserialize<KeyedBucket<object>>(json);

        // Assert
        Assert.NotNull(result);
        Assert.IsType<long>(result.Key);
        Assert.Equal(9_007_199_254_740_993L, result.Key);
    }

    [Fact]
    public void Read_WithDoubleValue_ReturnsDouble()
    {
        // Arrange
        var json = """{"key": 3.14, "keyAsString": "3.14"}""";

        // Act
        var result = _serializer.Deserialize<KeyedBucket<object>>(json);

        // Assert
        Assert.NotNull(result);
        Assert.IsType<double>(result.Key);
        Assert.Equal(3.14, result.Key);
    }

    [Fact]
    public void Read_WithStringContainingT_ButNotDateTime_ReturnsString()
    {
        // Arrange — "T" in a non-datetime string should NOT be parsed as a date
        var json = """{"key": "TeamAlpha", "keyAsString": "TeamAlpha"}""";

        // Act
        var result = _serializer.Deserialize<KeyedBucket<object>>(json);

        // Assert
        Assert.NotNull(result);
        Assert.IsType<string>(result.Key);
        Assert.Equal("TeamAlpha", result.Key);
    }

    [Fact]
    public void Read_WithTypedLongBucket_ReturnsLong()
    {
        // Arrange
        var json = """{"key": 42, "keyAsString": "42", "total": 10}""";

        // Act
        var result = _serializer.Deserialize<KeyedBucket<long>>(json);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(42L, result.Key);
        Assert.Equal(10, result.Total);
    }

    [Fact]
    public void Read_WithTypedStringBucket_ReturnsString()
    {
        // Arrange
        var json = """{"key": "status-active", "keyAsString": "status-active", "total": 5}""";

        // Act
        var result = _serializer.Deserialize<KeyedBucket<string>>(json);

        // Assert
        Assert.NotNull(result);
        Assert.Equal("status-active", result.Key);
        Assert.Equal(5, result.Total);
    }
}
