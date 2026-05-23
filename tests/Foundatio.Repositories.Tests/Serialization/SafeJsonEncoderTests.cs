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
    public void Serialize_Ampersand_EscapesAsUnicode()
    {
        // Arrange
        var obj = new { text = "a&b" };

        // Act
        string json = JsonSerializer.Serialize(obj, _optionsWithEncoder);

        // Assert
        Assert.Equal("{\"text\":\"a\\u0026b\"}", json);
    }

    [Fact]
    public void Serialize_AsciiLetters_DoesNotEscape()
    {
        // Arrange
        var obj = new { text = "Hello World 123" };

        // Act
        string json = JsonSerializer.Serialize(obj, _optionsWithEncoder);

        // Assert
        Assert.Equal("{\"text\":\"Hello World 123\"}", json);
    }

    [Fact]
    public void Serialize_DefaultEncoder_EscapesUnicode()
    {
        // Arrange
        var obj = new { text = "\u4e2d\u6587" };

        // Act
        string json = JsonSerializer.Serialize(obj, _optionsDefault);

        // Assert
        Assert.Equal("{\"text\":\"\\u4E2D\\u6587\"}", json);
    }

    [Fact]
    public void Serialize_HtmlAngleBrackets_EscapesAsUnicode()
    {
        // Arrange
        var obj = new { text = "<b>" };

        // Act
        string json = JsonSerializer.Serialize(obj, _optionsWithEncoder);

        // Assert
        Assert.Equal("{\"text\":\"\\u003Cb\\u003E\"}", json);
    }

    [Fact]
    public void Serialize_SupplementaryPlaneEmoji_EscapesAsSurrogatePairs()
    {
        // Arrange
        var obj = new { text = "\U0001F600" };

        // Act
        string json = JsonSerializer.Serialize(obj, _optionsWithEncoder);

        // Assert
        Assert.Equal("{\"text\":\"\\uD83D\\uDE00\"}", json);
    }

    [Fact]
    public void Serialize_UnicodeCharacters_PreservesUnescaped()
    {
        // Arrange
        var obj = new { text = "\u4e2d\u6587" };

        // Act
        string json = JsonSerializer.Serialize(obj, _optionsWithEncoder);

        // Assert
        Assert.Equal("{\"text\":\"\u4e2d\u6587\"}", json);
    }
}
