using System;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests.Extensions;

public class TimeSpanExtensionsTests
{
    [Theory]
    [InlineData(500, "500ms")]
    [InlineData(1, "1ms")]
    [InlineData(0, "0ms")]
    [InlineData(999, "999ms")]
    public void ToElasticDuration_SubSecond_ReturnsMilliseconds(int ms, string expected)
    {
        // Arrange
        var timeSpan = TimeSpan.FromMilliseconds(ms);

        // Act
        var result = timeSpan.ToElasticDuration();

        // Assert
        Assert.Equal(expected, result);
    }

    [Theory]
    [InlineData(1, "1s")]
    [InlineData(30, "30s")]
    [InlineData(59, "59s")]
    public void ToElasticDuration_WholeSeconds_ReturnsSeconds(int seconds, string expected)
    {
        // Arrange
        var timeSpan = TimeSpan.FromSeconds(seconds);

        // Act
        var result = timeSpan.ToElasticDuration();

        // Assert
        Assert.Equal(expected, result);
    }

    [Theory]
    [InlineData(1, "1m")]
    [InlineData(5, "5m")]
    [InlineData(59, "59m")]
    public void ToElasticDuration_WholeMinutes_ReturnsMinutes(int minutes, string expected)
    {
        // Arrange
        var timeSpan = TimeSpan.FromMinutes(minutes);

        // Act
        var result = timeSpan.ToElasticDuration();

        // Assert
        Assert.Equal(expected, result);
    }

    [Theory]
    [InlineData(1, "1h")]
    [InlineData(12, "12h")]
    [InlineData(23, "23h")]
    public void ToElasticDuration_WholeHours_ReturnsHours(int hours, string expected)
    {
        // Arrange
        var timeSpan = TimeSpan.FromHours(hours);

        // Act
        var result = timeSpan.ToElasticDuration();

        // Assert
        Assert.Equal(expected, result);
    }

    [Theory]
    [InlineData(1, "1d")]
    [InlineData(7, "7d")]
    [InlineData(30, "30d")]
    public void ToElasticDuration_WholeDays_ReturnsDays(int days, string expected)
    {
        // Arrange
        var timeSpan = TimeSpan.FromDays(days);

        // Act
        var result = timeSpan.ToElasticDuration();

        // Assert
        Assert.Equal(expected, result);
    }

    [Fact]
    public void ToElasticDuration_MixedSecondsAndMilliseconds_ReturnsMilliseconds()
    {
        // Arrange
        var timeSpan = TimeSpan.FromMilliseconds(1500);

        // Act
        var result = timeSpan.ToElasticDuration();

        // Assert
        Assert.Equal("1500ms", result);
    }

    [Fact]
    public void ToElasticDuration_MixedMinutesAndSeconds_ReturnsSeconds()
    {
        // Arrange
        var timeSpan = TimeSpan.FromSeconds(90);

        // Act
        var result = timeSpan.ToElasticDuration();

        // Assert
        Assert.Equal("90s", result);
    }
}
