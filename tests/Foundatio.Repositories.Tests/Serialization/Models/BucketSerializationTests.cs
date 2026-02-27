using System;
using System.Collections.Generic;
using System.Linq;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Tests.Utility;
using Foundatio.Serializer;
using Xunit;

namespace Foundatio.Repositories.Tests.Serialization.Models;

public class BucketSerializationTests
{
    [Fact]
    public void KeyedBucketString_WithInnerAggregations_RoundTrip()
    {
        // Arrange
        var innerAggs = new Dictionary<string, IAggregate>
        {
            ["avg_score"] = new ValueAggregate { Value = 4.5, Data = new Dictionary<string, object> { ["@type"] = "value" } }
        };
        var original = new BucketAggregate
        {
            Items = [new KeyedBucket<string>(innerAggs) { Key = "active", KeyAsString = "active", Total = 10, Data = new Dictionary<string, object> { ["@type"] = "string" } }],
            Total = 10,
            Data = new Dictionary<string, object> { ["@type"] = "bucket" }
        };
        var wrapper = new CountResult(10, new Dictionary<string, IAggregate> { ["terms"] = original });

        foreach (var serializer in SerializerTestHelper.GetTextSerializers())
        {
            // Act
            string json = serializer.SerializeToString(wrapper);
            var roundTripped = serializer.Deserialize<CountResult>(json);

            // Assert
            var bucket = roundTripped.Aggregations["terms"] as BucketAggregate;
            Assert.NotNull(bucket);
            var item = Assert.IsType<KeyedBucket<string>>(bucket.Items.First());
            Assert.Equal("active", item.Key);
            Assert.Equal(10, item.Total);
            Assert.Single(item.Aggregations);
            var avgScore = item.Aggregations["avg_score"] as ValueAggregate;
            Assert.NotNull(avgScore);
            Assert.Equal(4.5, avgScore.Value);
        }
    }

    [Fact]
    public void KeyedBucketDouble_RoundTrip()
    {
        // Arrange
        var original = new BucketAggregate
        {
            Items = [new KeyedBucket<double> { Key = 42.5, Total = 3, Data = new Dictionary<string, object> { ["@type"] = "double" } }],
            Total = 3,
            Data = new Dictionary<string, object> { ["@type"] = "bucket" }
        };
        var wrapper = new CountResult(3, new Dictionary<string, IAggregate> { ["histogram"] = original });

        foreach (var serializer in SerializerTestHelper.GetTextSerializers())
        {
            // Act
            string json = serializer.SerializeToString(wrapper);
            var roundTripped = serializer.Deserialize<CountResult>(json);

            // Assert
            var bucket = roundTripped.Aggregations["histogram"] as BucketAggregate;
            Assert.NotNull(bucket);
            var item = Assert.IsType<KeyedBucket<double>>(bucket.Items.First());
            Assert.Equal(42.5, item.Key);
            Assert.Equal(3, item.Total);
        }
    }

    [Fact]
    public void DateHistogramBucket_WithInnerAggregations_RoundTrip()
    {
        // Arrange
        var date = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        long epochMs = new DateTimeOffset(date).ToUnixTimeMilliseconds();
        var innerAggs = new Dictionary<string, IAggregate>
        {
            ["sum_amount"] = new ValueAggregate { Value = 1500.0, Data = new Dictionary<string, object> { ["@type"] = "value" } }
        };
        var original = new BucketAggregate
        {
            Items = [new DateHistogramBucket(date, innerAggs) { Key = epochMs, KeyAsString = "2026-01-01", Total = 5, Data = new Dictionary<string, object> { ["@type"] = "datehistogram" } }],
            Total = 5,
            Data = new Dictionary<string, object> { ["@type"] = "bucket" }
        };
        var wrapper = new CountResult(5, new Dictionary<string, IAggregate> { ["date_hist"] = original });

        foreach (var serializer in SerializerTestHelper.GetTextSerializers())
        {
            // Act
            string json = serializer.SerializeToString(wrapper);
            var roundTripped = serializer.Deserialize<CountResult>(json);

            // Assert
            var bucket = roundTripped.Aggregations["date_hist"] as BucketAggregate;
            Assert.NotNull(bucket);
            var item = Assert.IsType<DateHistogramBucket>(bucket.Items.First());
            Assert.Equal(date, item.Date);
            Assert.Equal(5, item.Total);
            Assert.Single(item.Aggregations);
            var sumAmount = item.Aggregations["sum_amount"] as ValueAggregate;
            Assert.NotNull(sumAmount);
            Assert.Equal(1500.0, sumAmount.Value);
        }
    }

    [Fact]
    public void RangeBucket_WithInnerAggregations_RoundTrip()
    {
        // Arrange
        var innerAggs = new Dictionary<string, IAggregate>
        {
            ["count_in_range"] = new ValueAggregate { Value = 25.0, Data = new Dictionary<string, object> { ["@type"] = "value" } }
        };
        var original = new BucketAggregate
        {
            Items =
            [
                new RangeBucket(innerAggs) { Key = "0-100", From = 0, FromAsString = "0", To = 100, ToAsString = "100", Total = 25, Data = new Dictionary<string, object> { ["@type"] = "range" } },
                new RangeBucket { Key = "100-*", From = 100, FromAsString = "100", Total = 5, Data = new Dictionary<string, object> { ["@type"] = "range" } }
            ],
            Total = 30,
            Data = new Dictionary<string, object> { ["@type"] = "bucket" }
        };
        var wrapper = new CountResult(30, new Dictionary<string, IAggregate> { ["price_ranges"] = original });

        foreach (var serializer in SerializerTestHelper.GetTextSerializers())
        {
            // Act
            string json = serializer.SerializeToString(wrapper);
            var roundTripped = serializer.Deserialize<CountResult>(json);

            // Assert
            var bucket = roundTripped.Aggregations["price_ranges"] as BucketAggregate;
            Assert.NotNull(bucket);
            Assert.Equal(2, bucket.Items.Count);

            var first = Assert.IsType<RangeBucket>(bucket.Items.First());
            Assert.Equal("0-100", first.Key);
            Assert.Equal(0, first.From);
            Assert.Equal(100, first.To);
            Assert.Equal(25, first.Total);
            Assert.Single(first.Aggregations);
            var countInRange = first.Aggregations["count_in_range"] as ValueAggregate;
            Assert.NotNull(countInRange);
            Assert.Equal(25.0, countInRange.Value);
        }
    }

    [Fact]
    public void SingleBucketAggregate_WithDeepNesting_RoundTrip()
    {
        // Arrange: sbucket -> bucket -> string keyed bucket with inner value agg
        var innerAggs = new Dictionary<string, IAggregate>
        {
            ["terms_status"] = new BucketAggregate
            {
                Items =
                [
                    new KeyedBucket<string>(new Dictionary<string, IAggregate>
                    {
                        ["min_val"] = new ValueAggregate { Value = 1.0, Data = new Dictionary<string, object> { ["@type"] = "value" } }
                    })
                    { Key = "approved", Total = 3, Data = new Dictionary<string, object> { ["@type"] = "string" } }
                ],
                Data = new Dictionary<string, object> { ["@type"] = "bucket" }
            }
        };
        var wrapper = new CountResult(10, new Dictionary<string, IAggregate>
        {
            ["nested_reviews"] = new SingleBucketAggregate(innerAggs) { Total = 5, Data = new Dictionary<string, object> { ["@type"] = "sbucket" } }
        });

        foreach (var serializer in SerializerTestHelper.GetTextSerializers())
        {
            // Act
            string json = serializer.SerializeToString(wrapper);
            var roundTripped = serializer.Deserialize<CountResult>(json);

            // Assert
            var nested = roundTripped.Aggregations["nested_reviews"] as SingleBucketAggregate;
            Assert.NotNull(nested);
            Assert.Equal(5, nested.Total);

            var terms = nested.Aggregations["terms_status"] as BucketAggregate;
            Assert.NotNull(terms);
            var approved = Assert.IsType<KeyedBucket<string>>(terms.Items.First());
            Assert.Equal("approved", approved.Key);

            var minVal = approved.Aggregations["min_val"] as ValueAggregate;
            Assert.NotNull(minVal);
            Assert.Equal(1.0, minVal.Value);
        }
    }
}
