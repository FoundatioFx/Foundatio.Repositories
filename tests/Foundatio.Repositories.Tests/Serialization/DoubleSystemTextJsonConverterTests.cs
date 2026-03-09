using System.Linq;
using System.Text.Json;
using Foundatio.Serializer;
using Xunit;

namespace Foundatio.Repositories.Tests.Serialization;

public class DoubleSystemTextJsonConverterTests
{
    private static readonly ITextSerializer _serializer =
        SerializerTestHelper.GetTextSerializers().OfType<SystemTextJsonSerializer>().First();

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
    public void Write_WithFractionalDouble_PreservesFullPrecision()
    {
        // Arrange
        double value = 3.14;

        // Act
        string json = _serializer.SerializeToString(value);

        // Assert
        Assert.Equal("3.14", json);
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

    [Fact]
    public void Write_WithNaN_WritesZero()
    {
        string json = _serializer.SerializeToString(double.NaN);
        Assert.Equal("0.0", json);
    }

    [Fact]
    public void Write_WithPositiveInfinity_WritesZero()
    {
        string json = _serializer.SerializeToString(double.PositiveInfinity);
        Assert.Equal("0.0", json);
    }

    [Fact]
    public void Write_WithNegativeInfinity_WritesZero()
    {
        string json = _serializer.SerializeToString(double.NegativeInfinity);
        Assert.Equal("0.0", json);
    }

    [Fact]
    public void Write_WithMaxValue_ProducesValidJson()
    {
        string json = _serializer.SerializeToString(double.MaxValue);
        double roundTripped = _serializer.Deserialize<double>(json);
        Assert.Equal(double.MaxValue, roundTripped);
    }

    [Fact]
    public void Write_WithMinValue_ProducesValidJson()
    {
        string json = _serializer.SerializeToString(double.MinValue);
        double roundTripped = _serializer.Deserialize<double>(json);
        Assert.Equal(double.MinValue, roundTripped);
    }

    [Fact]
    public void Write_WithEpsilon_ProducesValidJson()
    {
        string json = _serializer.SerializeToString(double.Epsilon);
        double roundTripped = _serializer.Deserialize<double>(json);
        Assert.Equal(double.Epsilon, roundTripped);
    }

    [Fact]
    public void Write_WithLargePrecisionDouble_PreservesValue()
    {
        double value = 123456789.123456;
        string json = _serializer.SerializeToString(value);
        double roundTripped = _serializer.Deserialize<double>(json);
        Assert.Equal(value, roundTripped);
    }

    [Fact]
    public void Write_WithNegativeWholeDouble_PreservesDecimalPoint()
    {
        string json = _serializer.SerializeToString(-5.0);
        Assert.Equal("-5.0", json);
    }

    [Theory]
    [InlineData(0.1)]
    [InlineData(0.001)]
    [InlineData(999999.999999)]
    [InlineData(-0.5)]
    public void RoundTrip_WithVariousFractions_PreservesValue(double value)
    {
        string json = _serializer.SerializeToString(value);
        double roundTripped = _serializer.Deserialize<double>(json);
        Assert.Equal(value, roundTripped);
    }
}
