using System.Collections.Generic;
using System.Text.Json;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class TaskPollingTests
{
    [Fact]
    public void RecordFailure_WithSuccessBetweenFailures_DoesNotReachLimit()
    {
        // Arrange
        var counter = new ConsecutiveFailureCounter(3);

        // Act
        bool reached = false;
        for (int i = 0; i < 10; i++)
        {
            reached |= counter.RecordFailure();
            reached |= counter.RecordFailure();
            counter.RecordSuccess();
        }

        // Assert
        Assert.False(reached);
        Assert.Equal(0, counter.Count);
    }

    [Fact]
    public void RecordFailure_WithConsecutiveFailures_ReachesLimit()
    {
        // Arrange
        var counter = new ConsecutiveFailureCounter(3);

        // Act & Assert
        Assert.False(counter.RecordFailure());
        Assert.False(counter.RecordFailure());
        Assert.True(counter.RecordFailure());
        Assert.Equal(3, counter.Count);
    }

    [Fact]
    public void Read_WithFailuresArray_DescribesEachFailure()
    {
        // Arrange
        using var document = JsonDocument.Parse("""{"failures":[{"index":"i","id":"1","cause":{"type":"script_exception","reason":"boom"},"status":400}],"updated":0}""");

        // Act
        var failures = TaskResponseFailures.Read(document.RootElement);

        // Assert
        Assert.Equal(["1: script_exception - boom"], failures);
    }

    [Fact]
    public void Read_WithEmptyFailures_ReturnsEmpty()
    {
        // Arrange
        using var document = JsonDocument.Parse("""{"failures":[],"updated":3}""");

        // Act & Assert
        Assert.Empty(TaskResponseFailures.Read(document.RootElement));
    }

    [Fact]
    public void Read_WithDictionaryResponse_ReadsFailures()
    {
        // Arrange
        using var document = JsonDocument.Parse("""[{"id":"7","cause":{"type":"t","reason":"r"}}]""");
        var response = new Dictionary<string, object> { ["failures"] = document.RootElement.Clone() };

        // Act & Assert
        Assert.Equal(["7: t - r"], TaskResponseFailures.Read(response));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void IsTimedOut_WithJsonAndDictionaryResponses_ReadsTimeoutFlag(bool timedOut)
    {
        // Arrange
        using var document = JsonDocument.Parse(timedOut ? """{"timed_out":true}""" : """{"timed_out":false}""");
        var response = new Dictionary<string, object> { ["timed_out"] = timedOut };
        var jsonResponse = new Dictionary<string, object> { ["timed_out"] = document.RootElement.GetProperty("timed_out").Clone() };

        // Act & Assert
        Assert.Equal(timedOut, TaskResponseFailures.IsTimedOut(document.RootElement));
        Assert.Equal(timedOut, TaskResponseFailures.IsTimedOut(response));
        Assert.Equal(timedOut, TaskResponseFailures.IsTimedOut(jsonResponse));
        Assert.False(TaskResponseFailures.IsTimedOut(null));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("""{"updated":3}""")]
    public void Read_WithoutFailures_ReturnsEmpty(string? json)
    {
        // Arrange
        object? response = json is null ? null : JsonDocument.Parse(json).RootElement;

        // Act & Assert
        Assert.Empty(TaskResponseFailures.Read(response));
    }

    [Fact]
    public void Summarize_WithManyFailures_ListsFirstFiveAndCountsRest()
    {
        // Arrange
        var failures = new[] { "a", "b", "c", "d", "e", "f", "g" };

        // Act & Assert
        Assert.Equal("a; b; c; d; e; and 2 more", TaskResponseFailures.Summarize(failures));
    }
}
