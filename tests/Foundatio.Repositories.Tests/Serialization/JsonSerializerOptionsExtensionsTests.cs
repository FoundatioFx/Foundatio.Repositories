using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;
using Foundatio.Repositories.Serialization;
using Foundatio.Serializer;
using Xunit;

namespace Foundatio.Repositories.Tests.Serialization;

public class JsonSerializerOptionsExtensionsTests
{
    [Fact]
    public void ConfigureDefaults_WithNewOptions_SetsDefaultIgnoreConditionNever()
    {
        // Arrange
        var options = new JsonSerializerOptions();

        // Act
        options.ConfigureFoundatioRepositoryDefaults();

        // Assert
        Assert.Equal(JsonIgnoreCondition.Never, options.DefaultIgnoreCondition);
    }

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
    public void ConfigureDefaults_WithPlainEnum_SerializesAsInteger()
    {
        var serializer = new SystemTextJsonSerializer(
            new JsonSerializerOptions().ConfigureFoundatioRepositoryDefaults());

        string json = serializer.SerializeToString(TestEnum.SomeValue);

        Assert.Equal("1", json);
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

    [Fact]
    public void ConfigureDefaults_SetsSafeJsonEncoder()
    {
        // Arrange
        var options = new JsonSerializerOptions();

        // Act
        options.ConfigureFoundatioRepositoryDefaults();

        // Assert
        Assert.Same(SafeJsonEncoder.Instance, options.Encoder);
    }

    [Fact]
    public void ConfigureDefaultsWithModifiers_SetsTypeInfoResolver()
    {
        // Arrange
        var options = new JsonSerializerOptions();

        // Act
        options.ConfigureFoundatioRepositoryDefaultsWithModifiers();

        // Assert
        Assert.NotNull(options.TypeInfoResolver);
    }

    [Fact]
    public void ConfigureDefaultsWithModifiers_SuppressesEmptyCollections()
    {
        // Arrange
        var options = new JsonSerializerOptions().ConfigureFoundatioRepositoryDefaultsWithModifiers();
        var obj = new { Name = "test", Items = new List<string>() };

        // Act
        string json = JsonSerializer.Serialize(obj, options);

        // Assert
        Assert.Contains("\"Name\"", json);
        Assert.DoesNotContain("Items", json);
    }

    [Fact]
    public void ConfigureDefaults_WithNullProperty_SerializesNullValue()
    {
        // Arrange
        var options = new JsonSerializerOptions().ConfigureFoundatioRepositoryDefaults();
        var obj = new { companyName = (string?)null, name = "test" };

        // Act
        string json = JsonSerializer.Serialize(obj, options);

        // Assert
        Assert.Contains("\"companyName\":null", json);
        Assert.Contains("\"name\":\"test\"", json);
    }

    private enum TestEnum
    {
        Default = 0,
        SomeValue = 1,
        AnotherValue = 2
    }
}
