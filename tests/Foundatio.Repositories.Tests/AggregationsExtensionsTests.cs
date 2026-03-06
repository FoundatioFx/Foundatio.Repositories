using System;
using System.Collections.Generic;
using System.Linq;
using Foundatio.Repositories.Models;
using Xunit;

namespace Foundatio.Repositories.Tests;

public sealed class AggregationsExtensionsTests
{
    [Fact]
    public void Terms_WithStringBuckets_PreservesDataProperty()
    {
        // Arrange
        var data = new Dictionary<string, object> { ["custom"] = "value" };
        var buckets = new List<IBucket>
        {
            new KeyedBucket<string> { Key = "bucket1", KeyAsString = "bucket1", Total = 10, Data = data }
        };
        var aggs = new Dictionary<string, IAggregate>
        {
            ["test"] = new BucketAggregate { Items = buckets }
        };

        // Act
        var result = aggs.Terms<string>("test");

        // Assert
        Assert.NotNull(result);
        var bucket = Assert.Single(result.Buckets);
        Assert.NotNull(bucket.Data);
        Assert.Equal("value", bucket.Data["custom"]);
    }

    [Fact]
    public void Terms_WithDoubleBuckets_ConvertsToStringKey()
    {
        // Arrange
        var data = new Dictionary<string, object> { ["metric"] = 42 };
        var buckets = new List<IBucket>
        {
            new KeyedBucket<double> { Key = 3.14, KeyAsString = "3.14", Total = 5, Data = data }
        };
        var aggs = new Dictionary<string, IAggregate>
        {
            ["test"] = new BucketAggregate { Items = buckets }
        };

        // Act
        var result = aggs.Terms<string>("test");

        // Assert
        Assert.NotNull(result);
        var bucket = Assert.Single(result.Buckets);
        Assert.Equal("3.14", bucket.Key);
        Assert.NotNull(bucket.Data);
        Assert.Equal(42, bucket.Data["metric"]);
    }

    [Fact]
    public void Terms_WithMultipleBucketTypes_HandlesAllTypes()
    {
        // Arrange
        var buckets = new List<IBucket>
        {
            new KeyedBucket<string> { Key = "str", KeyAsString = "str", Total = 1 },
            new KeyedBucket<double> { Key = 2.5, KeyAsString = "2.5", Total = 2 },
            new KeyedBucket<int> { Key = 3, KeyAsString = "3", Total = 3 },
            new KeyedBucket<long> { Key = 4L, KeyAsString = "4", Total = 4 },
            new KeyedBucket<bool> { Key = true, KeyAsString = "true", Total = 5 },
            new KeyedBucket<object> { Key = "obj", KeyAsString = "obj", Total = 6 }
        };
        var aggs = new Dictionary<string, IAggregate>
        {
            ["test"] = new BucketAggregate { Items = buckets }
        };

        // Act
        var result = aggs.Terms<string>("test");
        var resultBuckets = result.Buckets.ToList();

        // Assert
        Assert.NotNull(result);
        Assert.Equal(6, resultBuckets.Count);
        Assert.Equal("str", resultBuckets[0].Key);
        Assert.Equal("2.5", resultBuckets[1].Key);
        Assert.Equal("3", resultBuckets[2].Key);
        Assert.Equal("4", resultBuckets[3].Key);
        Assert.Equal("True", resultBuckets[4].Key);
        Assert.Equal("obj", resultBuckets[5].Key);
    }

    [Fact]
    public void Terms_WithNullData_ReturnsNullDataOnBucket()
    {
        // Arrange
        var buckets = new List<IBucket>
        {
            new KeyedBucket<string> { Key = "bucket1", KeyAsString = "bucket1", Total = 10, Data = null }
        };
        var aggs = new Dictionary<string, IAggregate>
        {
            ["test"] = new BucketAggregate { Items = buckets }
        };

        // Act
        var result = aggs.Terms<string>("test");

        // Assert
        Assert.NotNull(result);
        var bucket = Assert.Single(result.Buckets);
        Assert.Null(bucket.Data);
    }

    [Fact]
    public void Terms_WithNestedAggregations_PreservesAggregations()
    {
        // Arrange
        var innerAggs = new Dictionary<string, IAggregate>
        {
            ["sum"] = new ValueAggregate { Value = 100 }
        };
        var buckets = new List<IBucket>
        {
            new KeyedBucket<string>(innerAggs) { Key = "bucket1", KeyAsString = "bucket1", Total = 10 }
        };
        var aggs = new Dictionary<string, IAggregate>
        {
            ["test"] = new BucketAggregate { Items = buckets }
        };

        // Act
        var result = aggs.Terms<string>("test");

        // Assert
        Assert.NotNull(result);
        var bucket = Assert.Single(result.Buckets);
        Assert.NotNull(bucket.Aggregations);
        Assert.True(bucket.Aggregations.ContainsKey("sum"));
    }

    [Fact]
    public void Terms_WithNonExistentKey_ReturnsNull()
    {
        // Arrange
        var aggs = new Dictionary<string, IAggregate>();

        // Act
        var result = aggs.Terms<string>("nonexistent");

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public void Terms_WithEmptyBuckets_ReturnsEmptyCollection()
    {
        // Arrange
        var aggs = new Dictionary<string, IAggregate>
        {
            ["test"] = new BucketAggregate { Items = Array.Empty<IBucket>() }
        };

        // Act
        var result = aggs.Terms<string>("test");

        // Assert
        Assert.NotNull(result);
        Assert.Empty(result.Buckets);
    }

    [Fact]
    public void Terms_BucketAggregateData_IsPreservedOnTermsAggregate()
    {
        // Arrange
        var bucketData = new Dictionary<string, object> { ["total_other"] = 42 };
        var buckets = new List<IBucket>
        {
            new KeyedBucket<string> { Key = "bucket1", KeyAsString = "bucket1", Total = 10 }
        };
        var aggs = new Dictionary<string, IAggregate>
        {
            ["test"] = new BucketAggregate { Items = buckets, Data = bucketData }
        };

        // Act
        var result = aggs.Terms<string>("test");

        // Assert
        Assert.NotNull(result);
        Assert.NotNull(result.Data);
        Assert.Equal(42, result.Data["total_other"]);
    }

    [Fact]
    public void GeoHash_WithStringBuckets_PreservesDataProperty()
    {
        // Arrange
        var data = new Dictionary<string, object> { ["geo_info"] = "test" };
        var buckets = new List<IBucket>
        {
            new KeyedBucket<string> { Key = "u33dc1", KeyAsString = "u33dc1", Total = 5, Data = data }
        };
        var aggs = new Dictionary<string, IAggregate>
        {
            ["geo"] = new BucketAggregate { Items = buckets }
        };

        // Act
        var result = aggs.GeoHash("geo");

        // Assert
        Assert.NotNull(result);
        var bucket = Assert.Single(result.Buckets);
        Assert.NotNull(bucket.Data);
        Assert.Equal("test", bucket.Data["geo_info"]);
    }
}
