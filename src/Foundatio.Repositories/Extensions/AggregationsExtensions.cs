using System;
using System.Collections.Generic;
using System.Linq;

namespace Foundatio.Repositories.Models;

public static class AggregationsExtensions
{
    public static PercentileItem GetPercentile(this PercentilesAggregate agg, double percentile) => agg.Items.FirstOrDefault(i => i.Percentile == percentile);
    public static ValueAggregate Min(this IReadOnlyDictionary<string, IAggregate> aggs, string key) => aggs.TryGet<ValueAggregate>(key);
    public static ValueAggregate<T> Min<T>(this IReadOnlyDictionary<string, IAggregate> aggs, string key) => aggs.TryGet<ValueAggregate<T>>(key);

    public static ValueAggregate Max(this IReadOnlyDictionary<string, IAggregate> aggs, string key) => aggs.TryGet<ValueAggregate>(key);
    public static ValueAggregate<T> Max<T>(this IReadOnlyDictionary<string, IAggregate> aggs, string key) => aggs.TryGet<ValueAggregate<T>>(key);
    public static TopHitsAggregate TopHits(this IReadOnlyDictionary<string, IAggregate> aggs) => aggs.TryGet<TopHitsAggregate>("tophits");

    public static ValueAggregate Sum(this IReadOnlyDictionary<string, IAggregate> aggs, string key) => aggs.TryGet<ValueAggregate>(key);

    public static ValueAggregate Cardinality(this IReadOnlyDictionary<string, IAggregate> aggs, string key) => aggs.TryGet<ValueAggregate>(key);

    public static ValueAggregate Average(this IReadOnlyDictionary<string, IAggregate> aggs, string key) => aggs.TryGet<ValueAggregate>(key);

    public static ObjectValueAggregate Metric(this IReadOnlyDictionary<string, IAggregate> aggs, string key)
    {
        var valueMetric = aggs.TryGet<ValueAggregate>(key);

        return valueMetric != null
            ? new ObjectValueAggregate { Value = valueMetric.Value, Data = valueMetric.Data }
            : aggs.TryGet<ObjectValueAggregate>(key);
    }

    public static StatsAggregate Stats(this IReadOnlyDictionary<string, IAggregate> aggs, string key) => aggs.TryGet<StatsAggregate>(key);

    public static ExtendedStatsAggregate ExtendedStats(this IReadOnlyDictionary<string, IAggregate> aggs, string key) => aggs.TryGet<ExtendedStatsAggregate>(key);

    public static PercentilesAggregate Percentiles(this IReadOnlyDictionary<string, IAggregate> aggs, string key) => aggs.TryGet<PercentilesAggregate>(key);

    public static SingleBucketAggregate Missing(this IReadOnlyDictionary<string, IAggregate> aggs, string key) => aggs.TryGet<SingleBucketAggregate>(key);

    public static TermsAggregate<TKey> Terms<TKey>(this IReadOnlyDictionary<string, IAggregate> aggs, string key)
    {
        var bucket = aggs.TryGet<BucketAggregate>(key);
        return bucket == null
            ? null
            : new TermsAggregate<TKey>
            {
                Buckets = GetKeyedBuckets<TKey>(bucket.Items).ToList(),
                Data = bucket.Data
            };
    }

    public static TermsAggregate<string> Terms(this IReadOnlyDictionary<string, IAggregate> aggs, string key) => aggs.Terms<string>(key);

    public static MultiBucketAggregate<KeyedBucket<string>> GeoHash(this IReadOnlyDictionary<string, IAggregate> aggs, string key) => aggs.GetMultiKeyedBucketAggregate<string>(key);

    public static MultiBucketAggregate<DateHistogramBucket> DateHistogram(this IReadOnlyDictionary<string, IAggregate> aggs, string key) => aggs.GetMultiBucketAggregate<DateHistogramBucket>(key);

    private static TAggregate TryGet<TAggregate>(this IReadOnlyDictionary<string, IAggregate> aggs, string key)
        where TAggregate : class, IAggregate
    {
        return aggs.TryGetValue(key, out var agg) ? agg as TAggregate : null;
    }

    private static MultiBucketAggregate<TBucket> GetMultiBucketAggregate<TBucket>(this IReadOnlyDictionary<string, IAggregate> aggs, string key)
        where TBucket : IBucket
    {
        var bucket = aggs.TryGet<BucketAggregate>(key);
        if (bucket == null) return null;
        return new MultiBucketAggregate<TBucket>
        {
            Buckets = bucket.Items.OfType<TBucket>().ToList(),
            Data = bucket.Data
        };
    }

    private static MultiBucketAggregate<KeyedBucket<TKey>> GetMultiKeyedBucketAggregate<TKey>(this IReadOnlyDictionary<string, IAggregate> aggs, string key)
    {
        var bucket = aggs.TryGet<BucketAggregate>(key);
        if (bucket == null) return null;
        return new MultiBucketAggregate<KeyedBucket<TKey>>
        {
            Buckets = GetKeyedBuckets<TKey>(bucket.Items).ToList(),
            Data = bucket.Data
        };
    }

    private static IEnumerable<KeyedBucket<TKey>> GetKeyedBuckets<TKey>(IEnumerable<IBucket> items)
    {
        foreach (var item in items)
        {
            object key = null;
            string keyAsString = null;
            IReadOnlyDictionary<string, IAggregate> aggregations = null;
            long? total = null;

            switch (item)
            {
                case KeyedBucket<string> stringBucket:
                    key = stringBucket.Key;
                    keyAsString = stringBucket.KeyAsString;
                    aggregations = stringBucket.Aggregations;
                    total = stringBucket.Total;
                    break;
                case KeyedBucket<double> doubleBucket:
                    key = doubleBucket.Key;
                    keyAsString = doubleBucket.KeyAsString;
                    aggregations = doubleBucket.Aggregations;
                    total = doubleBucket.Total;
                    break;
                case KeyedBucket<long> longBucket:
                    key = longBucket.Key;
                    keyAsString = longBucket.KeyAsString;
                    aggregations = longBucket.Aggregations;
                    total = longBucket.Total;
                    break;
                case KeyedBucket<object> objectBucket:
                    key = objectBucket.Key;
                    keyAsString = objectBucket.KeyAsString;
                    aggregations = objectBucket.Aggregations;
                    total = objectBucket.Total;
                    break;
                default:
                    continue;
            }

            yield return new KeyedBucket<TKey>
            {
                Key = (TKey)Convert.ChangeType(key, typeof(TKey)),
                KeyAsString = keyAsString,
                Aggregations = aggregations,
                Total = total
            };
        }
    }
}
