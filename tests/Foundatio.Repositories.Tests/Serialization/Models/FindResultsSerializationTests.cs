using System.Collections.Generic;
using System.Linq;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Tests.Utility;
using Foundatio.Serializer;
using Xunit;

namespace Foundatio.Repositories.Tests.Serialization.Models;

public class FindResultsSerializationTests
{
    [Fact]
    public void FindResults_RoundTrip_PreservesAllProperties()
    {
        // Arrange
        var original = new FindResults<TestDocument>(
            hits: [new FindHit<TestDocument>("id1", new TestDocument { Name = "Test" }, 1.5)],
            total: 100
        );
        ((IFindResults<TestDocument>)original).Page = 2;
        ((IFindResults<TestDocument>)original).HasMore = true;

        foreach (var serializer in SerializerTestHelper.GetTextSerializers())
        {
            // Act
            string json = serializer.SerializeToString(original);
            var roundTripped = serializer.Deserialize<FindResults<TestDocument>>(json);

            // Assert
            Assert.Equal(original.Total, roundTripped.Total);
            Assert.Equal(original.Page, roundTripped.Page);
            Assert.Equal(original.HasMore, roundTripped.HasMore);
            Assert.Equal(original.Hits.Count, roundTripped.Hits.Count);
            Assert.Equal(original.Documents.Count, roundTripped.Documents.Count);
            Assert.Equal("Test", roundTripped.Documents.Single().Name);
        }
    }

    [Fact]
    public void CountResult_RoundTrip_PreservesAllProperties()
    {
        // Arrange
        var original = new CountResult(total: 42);

        foreach (var serializer in SerializerTestHelper.GetTextSerializers())
        {
            // Act
            string json = serializer.SerializeToString(original);
            var roundTripped = serializer.Deserialize<CountResult>(json);

            // Assert
            Assert.Equal(original.Total, roundTripped.Total);
        }
    }

    [Fact]
    public void CountResult_WithAggregations_RoundTrip()
    {
        // Arrange
        var aggregations = new Dictionary<string, IAggregate>
        {
            ["test_agg"] = new ValueAggregate { Value = 42.5, Data = new Dictionary<string, object> { ["@type"] = "value" } }
        };
        var original = new CountResult(42, aggregations);

        foreach (var serializer in SerializerTestHelper.GetTextSerializers())
        {
            // Act
            string json = serializer.SerializeToString(original);
            var roundTripped = serializer.Deserialize<CountResult>(json);

            // Assert
            Assert.Equal(original.Total, roundTripped.Total);
            Assert.Equal(original.Aggregations.Count, roundTripped.Aggregations.Count);
        }
    }

    [Fact]
    public void FindHit_RoundTrip_PreservesAllProperties()
    {
        // Arrange
        var original = new FindHit<TestDocument>("id1", new TestDocument { Name = "Test" }, 1.5, "v1", "routing1");

        foreach (var serializer in SerializerTestHelper.GetTextSerializers())
        {
            // Act
            string json = serializer.SerializeToString(original);
            var roundTripped = serializer.Deserialize<FindHit<TestDocument>>(json);

            // Assert
            Assert.Equal(original.Id, roundTripped.Id);
            Assert.Equal(original.Score, roundTripped.Score);
            Assert.Equal(original.Version, roundTripped.Version);
            Assert.Equal(original.Routing, roundTripped.Routing);
            Assert.Equal(original.Document.Name, roundTripped.Document.Name);
        }
    }

    [Fact]
    public void FindResults_EmptyHits_RoundTrip()
    {
        // Arrange
        var original = new FindResults<TestDocument>(total: 0);

        foreach (var serializer in SerializerTestHelper.GetTextSerializers())
        {
            // Act
            string json = serializer.SerializeToString(original);
            var roundTripped = serializer.Deserialize<FindResults<TestDocument>>(json);

            // Assert
            Assert.Equal(0, roundTripped.Total);
            Assert.Empty(roundTripped.Hits);
            Assert.Empty(roundTripped.Documents);
        }
    }

    [Fact]
    public void FindResults_WithMultipleHits_RoundTrip()
    {
        // Arrange
        var hits = new[]
        {
            new FindHit<TestDocument>("id1", new TestDocument { Name = "First" }, 2.5),
            new FindHit<TestDocument>("id2", new TestDocument { Name = "Second" }, 1.5),
            new FindHit<TestDocument>("id3", new TestDocument { Name = "Third" }, 0.5)
        };
        var original = new FindResults<TestDocument>(hits: hits, total: 3);

        foreach (var serializer in SerializerTestHelper.GetTextSerializers())
        {
            // Act
            string json = serializer.SerializeToString(original);
            var roundTripped = serializer.Deserialize<FindResults<TestDocument>>(json);

            // Assert
            Assert.Equal(3, roundTripped.Total);
            Assert.Equal(3, roundTripped.Hits.Count);
            Assert.Equal(3, roundTripped.Documents.Count);
            Assert.Equal("First", roundTripped.Documents.First().Name);
            Assert.Equal("Third", roundTripped.Documents.Last().Name);
        }
    }
}

public class TestDocument
{
    public string Id { get; set; }
    public string Name { get; set; }
}
