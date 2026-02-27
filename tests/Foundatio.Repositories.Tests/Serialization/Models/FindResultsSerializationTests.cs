using System;
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
    public void CountResult_WithAggregations_PreservesAllProperties()
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
    public void CountResult_WithSingleBucketAggregate_PreservesInnerAggregations()
    {
        // Arrange
        var innerAggs = new Dictionary<string, IAggregate>
        {
            ["terms_rating"] = new BucketAggregate
            {
                Items = [new KeyedBucket<string> { Key = "5", Total = 2, Data = new Dictionary<string, object> { ["@type"] = "string" } }],
                Data = new Dictionary<string, object> { ["@type"] = "bucket" }
            }
        };
        var original = new CountResult(3, new Dictionary<string, IAggregate>
        {
            ["nested_test"] = new SingleBucketAggregate(innerAggs) { Total = 6, Data = new Dictionary<string, object> { ["@type"] = "sbucket" } }
        });

        foreach (var serializer in SerializerTestHelper.GetTextSerializers())
        {
            // Act
            string json = serializer.SerializeToString(original);
            var roundTripped = serializer.Deserialize<CountResult>(json);

            // Assert
            var nested = roundTripped.Aggregations["nested_test"] as SingleBucketAggregate;
            Assert.NotNull(nested);
            Assert.Equal(6, nested.Total);
            Assert.Single(nested.Aggregations);
            Assert.True(nested.Aggregations.ContainsKey("terms_rating"), $"{serializer.GetType().Name}: inner aggregations lost");
        }
    }

    [Fact]
    public void CountResult_WithSingleBucketAggregate_NewtonsoftPreservesInnerAggregations()
    {
        // Arrange
        var innerAggs = new Dictionary<string, IAggregate>
        {
            ["terms_rating"] = new BucketAggregate
            {
                Items = [new KeyedBucket<string> { Key = "5", Total = 2, Data = new Dictionary<string, object> { ["@type"] = "string" } }],
                Data = new Dictionary<string, object> { ["@type"] = "bucket" }
            }
        };
        var original = new CountResult(3, new Dictionary<string, IAggregate>
        {
            ["nested_test"] = new SingleBucketAggregate(innerAggs) { Total = 6, Data = new Dictionary<string, object> { ["@type"] = "sbucket" } }
        });

        // Act
        var serializer = new JsonNetSerializer();
        string json = serializer.SerializeToString(original);
        var roundTripped = serializer.Deserialize<CountResult>(json);

        // Assert
        var nested = roundTripped.Aggregations["nested_test"] as SingleBucketAggregate;
        Assert.NotNull(nested);
        Assert.Equal(6, nested.Total);
        Assert.Single(nested.Aggregations);
        Assert.True(nested.Aggregations.ContainsKey("terms_rating"), "Newtonsoft: inner aggregations lost");
    }

    [Fact]
    public void CountResult_WithSingleBucketAggregate_SystemTextJsonPreservesInnerAggregations()
    {
        // Arrange
        var innerAggs = new Dictionary<string, IAggregate>
        {
            ["terms_rating"] = new BucketAggregate
            {
                Items = [new KeyedBucket<string> { Key = "5", Total = 2, Data = new Dictionary<string, object> { ["@type"] = "string" } }],
                Data = new Dictionary<string, object> { ["@type"] = "bucket" }
            }
        };
        var original = new CountResult(3, new Dictionary<string, IAggregate>
        {
            ["nested_test"] = new SingleBucketAggregate(innerAggs) { Total = 6, Data = new Dictionary<string, object> { ["@type"] = "sbucket" } }
        });

        // Act
        var serializer = new SystemTextJsonSerializer();
        string json = serializer.SerializeToString(original);
        var roundTripped = serializer.Deserialize<CountResult>(json);

        // Assert
        var nested = roundTripped.Aggregations["nested_test"] as SingleBucketAggregate;
        Assert.NotNull(nested);
        Assert.Equal(6, nested.Total);
        Assert.Single(nested.Aggregations);
        Assert.True(nested.Aggregations.ContainsKey("terms_rating"), "System.Text.Json: inner aggregations lost");
    }

    [Fact]
    public void CountResult_WithThreeLevelNesting_PreservesAllLevels()
    {
        // Arrange: sbucket -> sbucket -> terms bucket -> value agg (3 levels deep)
        var level3Aggs = new Dictionary<string, IAggregate>
        {
            ["avg_score"] = new ValueAggregate { Value = 4.5, Data = new Dictionary<string, object> { ["@type"] = "value" } }
        };
        var level2Aggs = new Dictionary<string, IAggregate>
        {
            ["terms_status"] = new BucketAggregate
            {
                Items = [new KeyedBucket<string>(level3Aggs) { Key = "active", Total = 7, Data = new Dictionary<string, object> { ["@type"] = "string" } }],
                Data = new Dictionary<string, object> { ["@type"] = "bucket" }
            }
        };
        var level1Aggs = new Dictionary<string, IAggregate>
        {
            ["nested_inner"] = new SingleBucketAggregate(level2Aggs) { Total = 10, Data = new Dictionary<string, object> { ["@type"] = "sbucket" } }
        };
        var original = new CountResult(100, new Dictionary<string, IAggregate>
        {
            ["nested_outer"] = new SingleBucketAggregate(level1Aggs) { Total = 50, Data = new Dictionary<string, object> { ["@type"] = "sbucket" } }
        });

        foreach (var serializer in SerializerTestHelper.GetTextSerializers())
        {
            // Act
            string json = serializer.SerializeToString(original);
            var roundTripped = serializer.Deserialize<CountResult>(json);

            // Assert - Level 1: outer sbucket
            var outer = roundTripped.Aggregations["nested_outer"] as SingleBucketAggregate;
            Assert.NotNull(outer);
            Assert.Equal(50, outer.Total);

            // Assert - Level 2: inner sbucket
            var inner = outer.Aggregations["nested_inner"] as SingleBucketAggregate;
            Assert.True(inner != null, $"{serializer.GetType().Name}: Level 2 SingleBucketAggregate lost");
            Assert.Equal(10, inner.Total);

            // Assert - Level 3: terms bucket with sub-agg
            var terms = inner.Aggregations["terms_status"] as BucketAggregate;
            Assert.True(terms != null, $"{serializer.GetType().Name}: Level 3 BucketAggregate lost");
            var bucket = Assert.IsType<KeyedBucket<string>>(terms.Items.First());
            Assert.Equal("active", bucket.Key);

            // Assert - Level 4: leaf value inside bucket
            var avgScore = bucket.Aggregations["avg_score"] as ValueAggregate;
            Assert.True(avgScore != null, $"{serializer.GetType().Name}: Level 4 ValueAggregate lost");
            Assert.Equal(4.5, avgScore.Value);
        }
    }

    [Fact]
    public void CountResult_WithMixedSubAggregations_PreservesAllTypes()
    {
        // Arrange: sbucket containing stats, percentiles, and a sub-bucket with value
        var bucketInnerAggs = new Dictionary<string, IAggregate>
        {
            ["min_age"] = new ValueAggregate { Value = 22.0, Data = new Dictionary<string, object> { ["@type"] = "value" } }
        };
        var nestedAggs = new Dictionary<string, IAggregate>
        {
            ["stats_rating"] = new StatsAggregate { Count = 10, Min = 1, Max = 5, Average = 3.5, Sum = 35, Data = new Dictionary<string, object> { ["@type"] = "stats" } },
            ["pct_score"] = new PercentilesAggregate([new PercentileItem { Percentile = 50, Value = 3.0 }, new PercentileItem { Percentile = 99, Value = 5.0 }])
            {
                Data = new Dictionary<string, object> { ["@type"] = "percentiles" }
            },
            ["terms_department"] = new BucketAggregate
            {
                Items = [new KeyedBucket<string>(bucketInnerAggs) { Key = "engineering", Total = 5, Data = new Dictionary<string, object> { ["@type"] = "string" } }],
                Data = new Dictionary<string, object> { ["@type"] = "bucket" }
            }
        };
        var original = new CountResult(100, new Dictionary<string, IAggregate>
        {
            ["nested_reviews"] = new SingleBucketAggregate(nestedAggs) { Total = 20, Data = new Dictionary<string, object> { ["@type"] = "sbucket" } }
        });

        foreach (var serializer in SerializerTestHelper.GetTextSerializers())
        {
            // Act
            string json = serializer.SerializeToString(original);
            var roundTripped = serializer.Deserialize<CountResult>(json);

            // Assert - sbucket preserved
            var nested = roundTripped.Aggregations["nested_reviews"] as SingleBucketAggregate;
            Assert.True(nested != null, $"{serializer.GetType().Name}: SingleBucketAggregate lost");
            Assert.Equal(3, nested.Aggregations.Count);

            // Assert - stats sub-agg
            var stats = nested.Aggregations["stats_rating"] as StatsAggregate;
            Assert.True(stats != null, $"{serializer.GetType().Name}: StatsAggregate sub-agg lost");
            Assert.Equal(3.5, stats.Average);

            // Assert - percentiles sub-agg
            var pct = nested.Aggregations["pct_score"] as PercentilesAggregate;
            Assert.True(pct != null, $"{serializer.GetType().Name}: PercentilesAggregate sub-agg lost");
            Assert.NotNull(pct.Items);
            Assert.Equal(2, pct.Items.Count);
            Assert.Equal(50, pct.Items.First().Percentile);

            // Assert - bucket sub-agg with its own sub-agg
            var terms = nested.Aggregations["terms_department"] as BucketAggregate;
            Assert.True(terms != null, $"{serializer.GetType().Name}: BucketAggregate sub-agg lost");
            var dept = Assert.IsType<KeyedBucket<string>>(terms.Items.First());
            Assert.Equal("engineering", dept.Key);
            var minAge = dept.Aggregations["min_age"] as ValueAggregate;
            Assert.True(minAge != null, $"{serializer.GetType().Name}: nested bucket ValueAggregate lost");
            Assert.Equal(22.0, minAge.Value);
        }
    }

    [Fact]
    public void CountResult_WithBucketAggregate_PreservesAllBuckets()
    {
        // Arrange
        var original = new CountResult(10, new Dictionary<string, IAggregate>
        {
            ["terms_status"] = new BucketAggregate
            {
                Items =
                [
                    new KeyedBucket<string> { Key = "active", Total = 7, Data = new Dictionary<string, object> { ["@type"] = "string" } },
                    new KeyedBucket<string> { Key = "inactive", Total = 3, Data = new Dictionary<string, object> { ["@type"] = "string" } }
                ],
                Total = 10,
                Data = new Dictionary<string, object> { ["@type"] = "bucket" }
            }
        });

        foreach (var serializer in SerializerTestHelper.GetTextSerializers())
        {
            // Act
            string json = serializer.SerializeToString(original);
            var roundTripped = serializer.Deserialize<CountResult>(json);

            // Assert
            var bucket = roundTripped.Aggregations["terms_status"] as BucketAggregate;
            Assert.NotNull(bucket);
            Assert.Equal(2, bucket.Items.Count);
            Assert.Equal(10, bucket.Total);

            var first = bucket.Items.First() as KeyedBucket<string>;
            Assert.NotNull(first);
            Assert.Equal("active", first.Key);
            Assert.Equal(7, first.Total);
        }
    }

    [Fact]
    public void CountResult_WithValueAggregate_PreservesValues()
    {
        // Arrange
        var original = new CountResult(5, new Dictionary<string, IAggregate>
        {
            ["max_age"] = new ValueAggregate { Value = 65.0, Data = new Dictionary<string, object> { ["@type"] = "value" } },
            ["min_age"] = new ValueAggregate { Value = 18.0, Data = new Dictionary<string, object> { ["@type"] = "value" } }
        });

        foreach (var serializer in SerializerTestHelper.GetTextSerializers())
        {
            // Act
            string json = serializer.SerializeToString(original);
            var roundTripped = serializer.Deserialize<CountResult>(json);

            // Assert
            var maxAge = roundTripped.Aggregations["max_age"] as ValueAggregate;
            Assert.NotNull(maxAge);
            Assert.Equal(65.0, maxAge.Value);

            var minAge = roundTripped.Aggregations["min_age"] as ValueAggregate;
            Assert.NotNull(minAge);
            Assert.Equal(18.0, minAge.Value);
        }
    }

    [Fact]
    public void CountResult_WithStatsAggregate_PreservesAllStats()
    {
        // Arrange
        var original = new CountResult(100, new Dictionary<string, IAggregate>
        {
            ["stats_age"] = new StatsAggregate { Count = 100, Min = 18, Max = 65, Average = 35.5, Sum = 3550, Data = new Dictionary<string, object> { ["@type"] = "stats" } }
        });

        foreach (var serializer in SerializerTestHelper.GetTextSerializers())
        {
            // Act
            string json = serializer.SerializeToString(original);
            var roundTripped = serializer.Deserialize<CountResult>(json);

            // Assert
            var stats = roundTripped.Aggregations["stats_age"] as StatsAggregate;
            Assert.NotNull(stats);
            Assert.Equal(100, stats.Count);
            Assert.Equal(18, stats.Min);
            Assert.Equal(65, stats.Max);
            Assert.Equal(35.5, stats.Average);
            Assert.Equal(3550, stats.Sum);
        }
    }

    [Fact]
    public void CountResult_WithPercentilesAggregate_PreservesItems()
    {
        // Arrange
        var original = new CountResult(50, new Dictionary<string, IAggregate>
        {
            ["percentiles_age"] = new PercentilesAggregate([new PercentileItem { Percentile = 50, Value = 30 }, new PercentileItem { Percentile = 95, Value = 60 }])
            {
                Data = new Dictionary<string, object> { ["@type"] = "percentiles" }
            }
        });

        foreach (var serializer in SerializerTestHelper.GetTextSerializers())
        {
            // Act
            string json = serializer.SerializeToString(original);
            var roundTripped = serializer.Deserialize<CountResult>(json);

            // Assert
            var percentiles = roundTripped.Aggregations["percentiles_age"] as PercentilesAggregate;
            Assert.NotNull(percentiles);
            Assert.NotNull(percentiles.Items);
            Assert.Equal(2, percentiles.Items.Count);
            Assert.Equal(50, percentiles.Items.First().Percentile);
            Assert.Equal(30, percentiles.Items.First().Value);
        }
    }

    [Fact]
    public void CountResult_WithObjectValueAggregate_PreservesValue()
    {
        // Arrange
        var original = new CountResult(1, new Dictionary<string, IAggregate>
        {
            ["obj_val"] = new ObjectValueAggregate { Value = "test_string", Data = new Dictionary<string, object> { ["@type"] = "ovalue" } }
        });

        foreach (var serializer in SerializerTestHelper.GetTextSerializers())
        {
            // Act
            string json = serializer.SerializeToString(original);
            var roundTripped = serializer.Deserialize<CountResult>(json);

            // Assert
            var objVal = roundTripped.Aggregations["obj_val"] as ObjectValueAggregate;
            Assert.NotNull(objVal);
            Assert.Equal("test_string", objVal.Value?.ToString());
        }
    }

    [Fact]
    public void CountResult_WithDateValueAggregate_PreservesDate()
    {
        // Arrange
        var date = new DateTime(2026, 1, 15, 0, 0, 0, DateTimeKind.Utc);
        var original = new CountResult(1, new Dictionary<string, IAggregate>
        {
            ["date_val"] = new ValueAggregate<DateTime> { Value = date, Data = new Dictionary<string, object> { ["@type"] = "dvalue" } }
        });

        foreach (var serializer in SerializerTestHelper.GetTextSerializers())
        {
            // Act
            string json = serializer.SerializeToString(original);
            var roundTripped = serializer.Deserialize<CountResult>(json);

            // Assert
            var dateVal = roundTripped.Aggregations["date_val"] as ValueAggregate<DateTime>;
            Assert.NotNull(dateVal);
            Assert.Equal(date, dateVal.Value);
        }
    }

    [Fact]
    public void CountResult_WithExtendedStatsAggregate_PreservesExtendedStats()
    {
        // Arrange
        var original = new CountResult(100, new Dictionary<string, IAggregate>
        {
            ["exstats_age"] = new ExtendedStatsAggregate
            {
                Count = 100, Min = 18, Max = 65, Average = 35.5, Sum = 3550,
                SumOfSquares = 150000, Variance = 200.5, StdDeviation = 14.16,
                Data = new Dictionary<string, object> { ["@type"] = "exstats" }
            }
        });

        foreach (var serializer in SerializerTestHelper.GetTextSerializers())
        {
            // Act
            string json = serializer.SerializeToString(original);
            var roundTripped = serializer.Deserialize<CountResult>(json);

            // Assert
            var exstats = roundTripped.Aggregations["exstats_age"] as ExtendedStatsAggregate;
            Assert.NotNull(exstats);
            Assert.Equal(100, exstats.Count);
            Assert.Equal(200.5, exstats.Variance);
            Assert.Equal(14.16, exstats.StdDeviation!.Value, 0.1);
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
    public void FindResults_EmptyHits_PreservesZeroState()
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
    public void FindResults_WithMultipleHits_PreservesAllDocuments()
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
