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
    public void ConfigureFoundatioRepositoryDefaults_NewOptions_SetsPropertyNameCaseInsensitive()
    {
        // Arrange
        var options = new JsonSerializerOptions();

        // Act
        options.ConfigureFoundatioRepositoryDefaults();

        // Assert
        Assert.True(options.PropertyNameCaseInsensitive);
    }

    [Fact]
    public void ConfigureFoundatioRepositoryDefaults_NewOptions_RegistersDoubleConverter()
    {
        // Arrange
        var options = new JsonSerializerOptions();

        // Act
        options.ConfigureFoundatioRepositoryDefaults();

        // Assert
        Assert.Contains(options.Converters, c => c is DoubleSystemTextJsonConverter);
    }

    [Fact]
    public void ConfigureFoundatioRepositoryDefaults_NewOptions_RegistersObjectConverter()
    {
        // Arrange
        var options = new JsonSerializerOptions();

        // Act
        options.ConfigureFoundatioRepositoryDefaults();

        // Assert
        Assert.Contains(options.Converters, c => c is ObjectToInferredTypesConverter);
    }

    [Fact]
    public void ConfigureFoundatioRepositoryDefaults_PlainEnum_SerializesAsInteger()
    {
        // Arrange
        var serializer = new SystemTextJsonSerializer(
            new JsonSerializerOptions().ConfigureFoundatioRepositoryDefaults());

        // Act
        string json = serializer.SerializeToString(TestEnum.SomeValue);

        // Assert
        Assert.Equal("1", json);
    }

    [Fact]
    public void ConfigureFoundatioRepositoryDefaults_MixedTypeObject_DeserializesValuesAsClrTypes()
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
    public void ConfigureFoundatioRepositoryDefaults_WholeDouble_PreservesDecimalPoint()
    {
        // Arrange
        var serializer = new SystemTextJsonSerializer(
            new JsonSerializerOptions().ConfigureFoundatioRepositoryDefaults());
        var obj = new { value = 1.0 };

        // Act
        string json = serializer.SerializeToString(obj);

        // Assert
        Assert.Equal("{\"value\":1.0}", json);
    }

    [Fact]
    public void ConfigureFoundatioRepositoryDefaults_DoesNotSetEncoder_LeavesDefault()
    {
        // Arrange
        var options = new JsonSerializerOptions();

        // Act
        options.ConfigureFoundatioRepositoryDefaults();

        // Assert
        Assert.Null(options.Encoder);
    }

    [Fact]
    public void ConfigureFoundatioRepositoryDefaults_NullProperty_SerializesNullValue()
    {
        // Arrange
        var options = new JsonSerializerOptions().ConfigureFoundatioRepositoryDefaults();
        var obj = new { companyName = (string?)null, name = "test" };

        // Act
        string json = JsonSerializer.Serialize(obj, options);

        // Assert
        Assert.Equal("{\"companyName\":null,\"name\":\"test\"}", json);
    }

    private enum TestEnum
    {
        Default = 0,
        SomeValue = 1,
        AnotherValue = 2
    }
}
