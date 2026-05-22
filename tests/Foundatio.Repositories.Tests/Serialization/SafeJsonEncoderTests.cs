using System.Text.Json;
using Foundatio.Repositories.Serialization;
using Xunit;

namespace Foundatio.Repositories.Tests.Serialization;

public class SafeJsonEncoderTests
{
    private static readonly JsonSerializerOptions _optionsWithEncoder = new()
    {
        Encoder = SafeJsonEncoder.Instance
    };

    private static readonly JsonSerializerOptions _optionsDefault = new();

    [Fact]
    public void Serialize_WithAmpersand_EscapesIt()
    {
        // Arrange
        var obj = new { text = "a&b" };

        // Act
        string json = JsonSerializer.Serialize(obj, _optionsWithEncoder);

        // Assert
        Assert.Contains("\\u0026", json);
    }

    [Fact]
    public void Serialize_WithAsciiLetters_DoesNotEscape()
    {
        // Arrange
        var obj = new { text = "Hello World 123" };

        // Act
        string json = JsonSerializer.Serialize(obj, _optionsWithEncoder);

        // Assert
        Assert.Contains("Hello World 123", json);
    }

    [Fact]
    public void Serialize_WithDefaultEncoder_EscapesUnicode()
    {
        // Arrange
        var obj = new { text = "\u4e2d\u6587" };

        // Act
        string json = JsonSerializer.Serialize(obj, _optionsDefault);

        // Assert
        Assert.Contains("\\u", json);
    }

    [Fact]
    public void Serialize_WithHtmlAngleBrackets_EscapesThem()
    {
        // Arrange
        var obj = new { text = "<script>alert('xss')</script>" };

        // Act
        string json = JsonSerializer.Serialize(obj, _optionsWithEncoder);

        // Assert
        Assert.DoesNotContain("<script>", json);
        Assert.Contains("\\u003C", json);
    }

    [Fact]
    public void Serialize_WithSupplementaryPlaneEmoji_EscapesAsSurrogatePairs()
    {
        // Arrange
        var obj = new { text = "\U0001F600" };

        // Act
        string json = JsonSerializer.Serialize(obj, _optionsWithEncoder);

        // Assert
        Assert.Contains("\\uD83D\\uDE00", json);
    }

    [Fact]
    public void Serialize_WithUnicodeCharacters_PreservesUnescaped()
    {
        // Arrange
        var obj = new { text = "\u4e2d\u6587" };

        // Act
        string json = JsonSerializer.Serialize(obj, _optionsWithEncoder);

        // Assert
        Assert.Contains("\u4e2d\u6587", json);
        Assert.DoesNotContain("\\u", json);
    }

    [Fact]
    public void ConfigureDefaults_WithNewOptions_SetsSafeEncoder()
    {
        // Arrange
        var options = new JsonSerializerOptions();

        // Act
        options.ConfigureFoundatioRepositoryDefaults();

        // Assert
        Assert.Same(SafeJsonEncoder.Instance, options.Encoder);
    }
}
