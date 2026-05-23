using System.Text.Json;
using Foundatio.Repositories.Serialization;
using Xunit;

namespace Foundatio.Repositories.Tests.Serialization;

public class JsonNumberInferenceTests
{
    [Fact]
    public void ReadNumber_JsonElementWithFloatingPoint_ReturnsDouble()
    {
        // Arrange
        using var doc = JsonDocument.Parse("42.5");

        // Act
        object result = JsonNumberInference.ReadNumber(doc.RootElement);

        // Assert
        Assert.IsType<double>(result);
        Assert.Equal(42.5, result);
    }

    [Fact]
    public void ReadNumber_JsonElementWithInteger_ReturnsLong()
    {
        // Arrange
        using var doc = JsonDocument.Parse("42");

        // Act
        object result = JsonNumberInference.ReadNumber(doc.RootElement);

        // Assert
        Assert.IsType<long>(result);
        Assert.Equal(42L, result);
    }

    [Fact]
    public void ReadNumber_JsonElementWithNegativeInteger_ReturnsLong()
    {
        // Arrange
        using var doc = JsonDocument.Parse("-42");

        // Act
        object result = JsonNumberInference.ReadNumber(doc.RootElement);

        // Assert
        Assert.IsType<long>(result);
        Assert.Equal(-42L, result);
    }

    [Fact]
    public void ReadNumber_JsonElementWithOverflowLong_ReturnsDouble()
    {
        // Arrange
        using var doc = JsonDocument.Parse("99999999999999999999");

        // Act
        object result = JsonNumberInference.ReadNumber(doc.RootElement);

        // Assert
        Assert.IsType<double>(result);
    }

    [Fact]
    public void ReadNumber_Utf8ReaderWithFloatingPoint_ReturnsDouble()
    {
        // Arrange
        var reader = CreateReader("42.5");
        reader.Read();

        // Act
        object result = JsonNumberInference.ReadNumber(ref reader);

        // Assert
        Assert.IsType<double>(result);
        Assert.Equal(42.5, result);
    }

    [Fact]
    public void ReadNumber_Utf8ReaderWithMaxLong_ReturnsLong()
    {
        // Arrange
        var reader = CreateReader(long.MaxValue.ToString());
        reader.Read();

        // Act
        object result = JsonNumberInference.ReadNumber(ref reader);

        // Assert
        Assert.IsType<long>(result);
        Assert.Equal(long.MaxValue, result);
    }

    [Fact]
    public void ReadNumber_Utf8ReaderWithNegativeInteger_ReturnsLong()
    {
        // Arrange
        var reader = CreateReader("-100");
        reader.Read();

        // Act
        object result = JsonNumberInference.ReadNumber(ref reader);

        // Assert
        Assert.IsType<long>(result);
        Assert.Equal(-100L, result);
    }

    [Fact]
    public void ReadNumber_Utf8ReaderWithOverflowLong_ReturnsDouble()
    {
        // Arrange
        string bigNumber = "99999999999999999999";
        var reader = CreateReader(bigNumber);
        reader.Read();

        // Act
        object result = JsonNumberInference.ReadNumber(ref reader);

        // Assert
        Assert.IsType<double>(result);
    }

    [Fact]
    public void ReadNumber_Utf8ReaderWithPositiveInteger_ReturnsLong()
    {
        // Arrange
        var reader = CreateReader("42");
        reader.Read();

        // Act
        object result = JsonNumberInference.ReadNumber(ref reader);

        // Assert
        Assert.IsType<long>(result);
        Assert.Equal(42L, result);
    }

    [Fact]
    public void ReadNumber_Utf8ReaderWithScientificNotation_ReturnsDouble()
    {
        // Arrange
        var reader = CreateReader("1.5e3");
        reader.Read();

        // Act
        object result = JsonNumberInference.ReadNumber(ref reader);

        // Assert
        Assert.IsType<double>(result);
        Assert.Equal(1500.0, result);
    }

    [Fact]
    public void ReadNumber_Utf8ReaderWithZero_ReturnsLong()
    {
        // Arrange
        var reader = CreateReader("0");
        reader.Read();

        // Act
        object result = JsonNumberInference.ReadNumber(ref reader);

        // Assert
        Assert.IsType<long>(result);
        Assert.Equal(0L, result);
    }

    private static Utf8JsonReader CreateReader(string json)
    {
        var bytes = System.Text.Encoding.UTF8.GetBytes(json);
        return new Utf8JsonReader(bytes);
    }
}
