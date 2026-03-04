using System.Text.Json;
using Foundatio.Repositories.Serialization;
using Foundatio.Serializer;
using Xunit;

namespace Foundatio.Repositories.Tests.Serialization;

public class DoubleSystemTextJsonConverterTests
{
    private static readonly ITextSerializer _serializer = new SystemTextJsonSerializer(
        new JsonSerializerOptions().ConfigureFoundatioRepositoryDefaults());

    [Fact]
    public void Write_WithWholeDouble_PreservesDecimalPoint()
    {
        // Arrange
        double value = 1.0;

        // Act
        string json = _serializer.SerializeToString(value);

        // Assert
        Assert.Equal("1.0", json);
    }

    [Fact]
    public void Write_WithZero_PreservesDecimalPoint()
    {
        // Arrange
        double value = 0.0;

        // Act
        string json = _serializer.SerializeToString(value);

        // Assert
        Assert.Equal("0.0", json);
    }

    [Fact]
    public void Write_WithFractionalDouble_PreservesValue()
    {
        // Arrange
        double value = 3.14;

        // Act
        string json = _serializer.SerializeToString(value);

        // Assert
        Assert.StartsWith("3.1", json);
    }

    [Fact]
    public void Read_WithWholeNumber_ReturnsDouble()
    {
        // Arrange
        string json = "1";

        // Act
        double result = _serializer.Deserialize<double>(json);

        // Assert
        Assert.Equal(1.0, result);
    }

    [Fact]
    public void Read_WithDecimalNumber_ReturnsExactDouble()
    {
        // Arrange
        string json = "3.14";

        // Act
        double result = _serializer.Deserialize<double>(json);

        // Assert
        Assert.Equal(3.14, result);
    }

    [Fact]
    public void RoundTrip_WithWholeDouble_PreservesDecimalRepresentation()
    {
        // Arrange
        double original = 42.0;

        // Act
        string json = _serializer.SerializeToString(original);
        double roundTripped = _serializer.Deserialize<double>(json);

        // Assert
        Assert.Contains(".", json);
        Assert.Equal(original, roundTripped);
    }

    [Fact]
    public void RoundTrip_WithNegativeDouble_PreservesValue()
    {
        // Arrange
        double original = -99.5;

        // Act
        string json = _serializer.SerializeToString(original);
        double roundTripped = _serializer.Deserialize<double>(json);

        // Assert
        Assert.Equal(original, roundTripped);
    }

    [Fact]
    public void Write_WithDoubleInObject_PreservesDecimalPoint()
    {
        // Arrange
        var obj = new { Value = 1.0 };

        // Act
        string json = _serializer.SerializeToString(obj);

        // Assert
        Assert.Contains("1.0", json);
    }
}
