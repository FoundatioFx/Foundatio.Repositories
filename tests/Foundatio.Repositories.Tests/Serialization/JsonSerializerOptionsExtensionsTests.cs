using System.Collections.Generic;
using System.Text.Json;
using Foundatio.Repositories.Serialization;
using Foundatio.Serializer;
using Xunit;

namespace Foundatio.Repositories.Tests.Serialization;

public class JsonSerializerOptionsExtensionsTests
{
    [Fact]
    public void ConfigureDefaults_WithNewOptions_SetsPropertyNameCaseInsensitive()
    {
        // Arrange
        var options = new JsonSerializerOptions();

        // Act
        options.ConfigureFoundatioRepositoryDefaults();

        // Assert
        Assert.True(options.PropertyNameCaseInsensitive);
    }

    [Fact]
    public void ConfigureDefaults_WithNewOptions_RegistersDoubleConverter()
    {
        // Arrange
        var options = new JsonSerializerOptions();

        // Act
        options.ConfigureFoundatioRepositoryDefaults();

        // Assert
        Assert.Contains(options.Converters, c => c is DoubleSystemTextJsonConverter);
    }

    [Fact]
    public void ConfigureDefaults_WithNewOptions_RegistersObjectConverter()
    {
        // Arrange
        var options = new JsonSerializerOptions();

        // Act
        options.ConfigureFoundatioRepositoryDefaults();

        // Assert
        Assert.Contains(options.Converters, c => c is ObjectToInferredTypesConverter);
    }

    [Fact]
    public void ConfigureDefaults_WithEnumValue_SerializesAsCamelCase()
    {
        // Arrange
        var serializer = new SystemTextJsonSerializer(
            new JsonSerializerOptions().ConfigureFoundatioRepositoryDefaults());

        // Act
        string json = serializer.SerializeToString(TestEnum.SomeValue);

        // Assert
        Assert.Equal("\"someValue\"", json);
    }

    [Fact]
    public void ConfigureDefaults_WithCamelCaseEnumString_DeserializesToEnum()
    {
        // Arrange
        var serializer = new SystemTextJsonSerializer(
            new JsonSerializerOptions().ConfigureFoundatioRepositoryDefaults());

        // Act
        var result = serializer.Deserialize<TestEnum>("\"someValue\"");

        // Assert
        Assert.Equal(TestEnum.SomeValue, result);
    }

    [Fact]
    public void ConfigureDefaults_WithIntegerEnumValue_DeserializesToEnum()
    {
        // Arrange
        var serializer = new SystemTextJsonSerializer(
            new JsonSerializerOptions().ConfigureFoundatioRepositoryDefaults());

        // Act
        var result = serializer.Deserialize<TestEnum>("1");

        // Assert
        Assert.Equal(TestEnum.SomeValue, result);
    }

    [Fact]
    public void ConfigureDefaults_WithMixedTypeObject_DeserializesObjectValuesAsClrTypes()
    {
        // Arrange
        var serializer = new SystemTextJsonSerializer(
            new JsonSerializerOptions().ConfigureFoundatioRepositoryDefaults());
        string json = "{\"name\":\"test\",\"count\":42,\"active\":true,\"score\":1.5}";

        // Act
        var result = serializer.Deserialize<Dictionary<string, object>>(json);

        // Assert
        Assert.NotNull(result);
        Assert.IsType<string>(result["name"]);
        Assert.IsType<long>(result["count"]);
        Assert.IsType<bool>(result["active"]);
        Assert.IsType<double>(result["score"]);
    }

    [Fact]
    public void ConfigureDefaults_WithWholeDouble_PreservesDecimalPointInRoundTrip()
    {
        // Arrange
        var serializer = new SystemTextJsonSerializer(
            new JsonSerializerOptions().ConfigureFoundatioRepositoryDefaults());
        var obj = new { value = 1.0 };

        // Act
        string json = serializer.SerializeToString(obj);

        // Assert
        Assert.Contains("1.0", json);
    }

    private enum TestEnum
    {
        Default = 0,
        SomeValue = 1,
        AnotherValue = 2
    }
}
