using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.AsyncSearch;
using Elastic.Clients.Elasticsearch.Core.Bulk;
using Elastic.Clients.Elasticsearch.Core.Search;
using Elastic.Clients.Elasticsearch.Mapping;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.CustomFields;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;
using Foundatio.Serializer;
using Foundatio.Utility;
using Microsoft.Extensions.Logging;
using ElasticAggregations = Elastic.Clients.Elasticsearch.Aggregations;

namespace Foundatio.Repositories.Elasticsearch.Extensions;

public static class ElasticIndexExtensions
{
    public static SubmitAsyncSearchRequest ToAsyncSearchSubmitRequest<T>(this SearchRequest searchRequest) where T : class, new()
    {
        var asyncSearchRequest = new SubmitAsyncSearchRequest(searchRequest.Indices)
        {
            Aggregations = searchRequest.Aggregations,
            Collapse = searchRequest.Collapse,
            DocvalueFields = searchRequest.DocvalueFields,
            Explain = searchRequest.Explain,
            From = searchRequest.From,
            Highlight = searchRequest.Highlight,
            IndicesBoost = searchRequest.IndicesBoost,
            MinScore = searchRequest.MinScore,
            PostFilter = searchRequest.PostFilter,
            Profile = searchRequest.Profile,
            Query = searchRequest.Query,
            Rescore = searchRequest.Rescore,
            ScriptFields = searchRequest.ScriptFields,
            SearchAfter = searchRequest.SearchAfter,
            Size = searchRequest.Size,
            Sort = searchRequest.Sort,
            Source = searchRequest.Source,
            StoredFields = searchRequest.StoredFields,
            Suggest = searchRequest.Suggest,
            TerminateAfter = searchRequest.TerminateAfter,
            Timeout = searchRequest.Timeout,
            TrackScores = searchRequest.TrackScores,
            TrackTotalHits = searchRequest.TrackTotalHits,
            Version = searchRequest.Version,
            RuntimeMappings = searchRequest.RuntimeMappings,
            SeqNoPrimaryTerm = searchRequest.SeqNoPrimaryTerm
        };

        return asyncSearchRequest;
    }

    public static FindResults<T> ToFindResults<T>(this SearchResponse<T> response, ICommandOptions options, ITextSerializer serializer) where T : class, new()
    {
        ArgumentNullException.ThrowIfNull(response);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(serializer);

        if (!response.IsValidResponse)
        {
            if (response.ApiCallDetails.HttpStatusCode.GetValueOrDefault() == 404)
                return new FindResults<T>();

            throw new DocumentException(response.GetErrorMessage("Error while searching"), response.OriginalException());
        }

        int limit = options.GetLimit();
        var docs = response.Hits.Take(limit).ToFindHits().ToList();

        var data = new DataDictionary();
        if (response.ScrollId != null)
            data.Add(ElasticDataKeys.ScrollId, response.ScrollId.ToString());

        var results = new FindResults<T>(docs, response.Total, response.ToAggregations(serializer), null, data);
        var protectedResults = (IFindResults<T>)results;
        if (options.ShouldUseSnapshotPaging())
            protectedResults.HasMore = response.Hits.Count >= limit;
        else
            protectedResults.HasMore = response.Hits.Count > limit || response.Hits.Count >= options.GetMaxLimit();

        if (options.HasSearchAfter())
        {
            results.SetSearchBeforeToken(serializer);
            if (results.HasMore)
                results.SetSearchAfterToken(serializer);
        }
        else if (options.HasSearchBefore())
        {
            // reverse results
            protectedResults.Reverse();
            results.SetSearchAfterToken(serializer);
            if (results.HasMore)
                results.SetSearchBeforeToken(serializer);
        }
        else if (results.HasMore)
        {
            results.SetSearchAfterToken(serializer);
        }

        protectedResults.Page = options.GetPage();

        return results;
    }

    public static FindResults<T> ToFindResults<T>(this SubmitAsyncSearchResponse<T> response, ICommandOptions options, ITextSerializer serializer) where T : class, new()
    {
        ArgumentNullException.ThrowIfNull(response);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(serializer);

        if (!response.IsValidResponse)
        {
            if (response.ApiCallDetails.HttpStatusCode.GetValueOrDefault() == 404)
                return new FindResults<T>();

            throw new DocumentException(response.GetErrorMessage("Error while searching"), response.OriginalException());
        }

        int limit = options.GetLimit();
        var docs = response.Response.Hits.Take(limit).ToFindHits().ToList();

        var data = new DataDictionary
        {
            { AsyncQueryDataKeys.AsyncQueryId, response.Id },
            { AsyncQueryDataKeys.IsRunning, response.IsRunning },
            { AsyncQueryDataKeys.IsPartial, response.IsPartial }
        };

        if (options.ShouldAutoDeleteAsyncQuery() && !response.IsRunning)
            data.Remove(AsyncQueryDataKeys.AsyncQueryId);

        var results = new FindResults<T>(docs, response.Response.Total, response.ToAggregations(serializer), null, data);
        var protectedResults = (IFindResults<T>)results;
        if (options.ShouldUseSnapshotPaging())
            protectedResults.HasMore = response.Response.Hits.Count >= limit;
        else
            protectedResults.HasMore = response.Response.Hits.Count > limit || response.Response.Hits.Count >= options.GetMaxLimit();

        if (options.HasSearchAfter())
        {
            results.SetSearchBeforeToken(serializer);
            if (results.HasMore)
                results.SetSearchAfterToken(serializer);
        }
        else if (options.HasSearchBefore())
        {
            // reverse results
            protectedResults.Reverse();
            results.SetSearchAfterToken(serializer);
            if (results.HasMore)
                results.SetSearchBeforeToken(serializer);
        }
        else if (results.HasMore)
        {
            results.SetSearchAfterToken(serializer);
        }

        protectedResults.Page = options.GetPage();

        return results;
    }

    public static IEnumerable<FindHit<T>> ToFindHits<T>(this IEnumerable<Hit<T>> hits) where T : class
    {
        return hits.Select(h => h.ToFindHit());
    }

    public static CountResult ToCountResult<T>(this SearchResponse<T> response, ICommandOptions options, ITextSerializer serializer) where T : class, new()
    {
        if (!response.IsValidResponse)
        {
            if (response.ApiCallDetails.HttpStatusCode.GetValueOrDefault() == 404)
                return new FindResults<T>();

            throw new DocumentException(response.GetErrorMessage("Error while counting"), response.OriginalException());
        }

        var data = new DataDictionary();
        if (response.ScrollId != null)
            data.Add(ElasticDataKeys.ScrollId, response.ScrollId.ToString());

        return new CountResult(response.Total, response.ToAggregations(serializer), data);
    }

    public static CountResult ToCountResult<T>(this SubmitAsyncSearchResponse<T> response, ICommandOptions options, ITextSerializer serializer) where T : class, new()
    {
        if (!response.IsValidResponse)
        {
            if (response.ApiCallDetails.HttpStatusCode.GetValueOrDefault() == 404)
                return new FindResults<T>();

            throw new DocumentException(response.GetErrorMessage("Error while counting"), response.OriginalException());
        }

        var data = new DataDictionary
        {
            { AsyncQueryDataKeys.AsyncQueryId, response.Id },
            { AsyncQueryDataKeys.IsRunning, response.IsRunning },
            { AsyncQueryDataKeys.IsPartial, response.IsPartial }
        };

        if (options.ShouldAutoDeleteAsyncQuery() && !response.IsRunning)
            data.Remove(AsyncQueryDataKeys.AsyncQueryId);

        return new CountResult(response.Response.Total, response.ToAggregations(serializer), data);
    }

    public static FindResults<T> ToFindResults<T>(this GetAsyncSearchResponse<T> response, ICommandOptions options, ITextSerializer serializer) where T : class, new()
    {
        ArgumentNullException.ThrowIfNull(response);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(serializer);

        if (!response.IsValidResponse)
        {
            if (response.ApiCallDetails.HttpStatusCode.GetValueOrDefault() == 404)
                return new FindResults<T>();

            throw new DocumentException(response.GetErrorMessage("Error while searching"), response.OriginalException());
        }

        int limit = options.GetLimit();
        var docs = response.Response.Hits.Take(limit).ToFindHits().ToList();

        var data = new DataDictionary
        {
            { AsyncQueryDataKeys.AsyncQueryId, response.Id },
            { AsyncQueryDataKeys.IsRunning, response.IsRunning },
            { AsyncQueryDataKeys.IsPartial, response.IsPartial }
        };

        if (options.ShouldAutoDeleteAsyncQuery() && !response.IsRunning)
            data.Remove(AsyncQueryDataKeys.AsyncQueryId);

        var results = new FindResults<T>(docs, response.Response.Total, response.ToAggregations(serializer), null, data);
        var protectedResults = (IFindResults<T>)results;
        if (options.ShouldUseSnapshotPaging())
            protectedResults.HasMore = response.Response.Hits.Count >= limit;
        else
            protectedResults.HasMore = response.Response.Hits.Count > limit || response.Response.Hits.Count >= options.GetMaxLimit();

        if (options.HasSearchAfter())
        {
            results.SetSearchBeforeToken(serializer);
            if (results.HasMore)
                results.SetSearchAfterToken(serializer);
        }
        else if (options.HasSearchBefore())
        {
            protectedResults.Reverse();
            results.SetSearchAfterToken(serializer);
            if (results.HasMore)
                results.SetSearchBeforeToken(serializer);
        }
        else if (results.HasMore)
        {
            results.SetSearchAfterToken(serializer);
        }

        protectedResults.Page = options.GetPage();

        return results;
    }

    public static CountResult ToCountResult<T>(this GetAsyncSearchResponse<T> response, ICommandOptions options, ITextSerializer serializer) where T : class, new()
    {
        if (!response.IsValidResponse)
        {
            if (response.ApiCallDetails.HttpStatusCode.GetValueOrDefault() == 404)
                return new FindResults<T>();

            throw new DocumentException(response.GetErrorMessage("Error while counting"), response.OriginalException());
        }

        var data = new DataDictionary
        {
            { AsyncQueryDataKeys.AsyncQueryId, response.Id },
            { AsyncQueryDataKeys.IsRunning, response.IsRunning },
            { AsyncQueryDataKeys.IsPartial, response.IsPartial }
        };

        if (options.ShouldAutoDeleteAsyncQuery() && !response.IsRunning)
            data.Remove(AsyncQueryDataKeys.AsyncQueryId);

        return new CountResult(response.Response.Total, response.ToAggregations(serializer), data);
    }

    public static FindResults<T> ToFindResults<T>(this ScrollResponse<T> response, ICommandOptions options, ITextSerializer serializer) where T : class, new()
    {
        ArgumentNullException.ThrowIfNull(response);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(serializer);

        if (!response.IsValidResponse)
        {
            if (response.ApiCallDetails.HttpStatusCode.GetValueOrDefault() == 404)
                return new FindResults<T>();

            throw new DocumentException(response.GetErrorMessage("Error while searching"), response.OriginalException());
        }

        int limit = options.GetLimit();
        var docs = response.Hits.Take(limit).ToFindHits().ToList();

        var data = new DataDictionary();
        if (response.ScrollId != null)
            data.Add(ElasticDataKeys.ScrollId, response.ScrollId.ToString());

        var results = new FindResults<T>(docs, response.Total, response.ToAggregations(serializer), null, data);
        var protectedResults = (IFindResults<T>)results;
        protectedResults.HasMore = response.Hits.Count > 0 && response.Hits.Count >= limit;

        protectedResults.Page = options.GetPage();

        return results;
    }

    public static FindHit<T> ToFindHit<T>(this GetResponse<T> hit) where T : class
    {
        var data = new DataDictionary { { ElasticDataKeys.Index, hit.Index } };

        if (hit.Source is IVersioned versionedDoc)
            versionedDoc.Version = hit.GetElasticVersion();

        return new FindHit<T>(hit.Id, hit.Source, 0, hit.GetElasticVersion(), hit.Routing, data);
    }

    public static ElasticDocumentVersion GetElasticVersion<T>(this GetResponse<T> hit) where T : class
    {
        if (!hit.PrimaryTerm.HasValue || !hit.SeqNo.HasValue)
            return ElasticDocumentVersion.Empty;

        if (hit.PrimaryTerm.Value == 0 && hit.SeqNo.Value == 0)
            return ElasticDocumentVersion.Empty;

        return new ElasticDocumentVersion(hit.PrimaryTerm.Value, hit.SeqNo.Value);
    }

    public static ElasticDocumentVersion GetElasticVersion<T>(this Hit<T> hit) where T : class
    {
        if (!hit.PrimaryTerm.HasValue || !hit.SeqNo.HasValue)
            return ElasticDocumentVersion.Empty;

        if (hit.PrimaryTerm.Value == 0 && hit.SeqNo.Value == 0)
            return ElasticDocumentVersion.Empty;

        return new ElasticDocumentVersion(hit.PrimaryTerm.Value, hit.SeqNo.Value);
    }

    public static ElasticDocumentVersion GetElasticVersion<T>(this FindHit<T> hit) where T : class
    {
        if (hit == null || String.IsNullOrEmpty(hit.Version))
            return ElasticDocumentVersion.Empty;

        return hit.Version;
    }

    public static ElasticDocumentVersion GetElasticVersion(this IndexResponse hit)
    {
        if (hit.PrimaryTerm.GetValueOrDefault() == 0 && hit.SeqNo.GetValueOrDefault() == 0)
            return ElasticDocumentVersion.Empty;

        return new ElasticDocumentVersion(hit.PrimaryTerm.GetValueOrDefault(), hit.SeqNo.GetValueOrDefault());
    }

    public static ElasticDocumentVersion GetElasticVersion(this ResponseItem hit)
    {
        if (hit.PrimaryTerm.GetValueOrDefault() == 0 && hit.SeqNo.GetValueOrDefault() == 0)
            return ElasticDocumentVersion.Empty;

        return new ElasticDocumentVersion(hit.PrimaryTerm.GetValueOrDefault(), hit.SeqNo.GetValueOrDefault());
    }

    public static ElasticDocumentVersion GetElasticVersion(this IVersioned versioned)
    {
        if (versioned == null || String.IsNullOrEmpty(versioned.Version))
            return ElasticDocumentVersion.Empty;

        return versioned.Version;
    }

    public static FindHit<T> ToFindHit<T>(this Hit<T> hit) where T : class
    {
        var data = new DataDictionary {
            { ElasticDataKeys.Index, hit.Index }
        };

        // Only add sorts if they exist
        if (hit.Sort != null && hit.Sort.Count > 0)
            data[ElasticDataKeys.Sorts] = hit.Sort;

        if (hit.Source is IVersioned versionedDoc && hit.PrimaryTerm.HasValue)
            versionedDoc.Version = hit.GetElasticVersion();

        return new FindHit<T>(hit.Id, hit.Source, hit.Score.GetValueOrDefault(), hit.GetElasticVersion(), hit.Routing, data);
    }

    public static IEnumerable<FindHit<T>> ToFindHits<T>(this MultiGetResponse<T> response, ILogger logger = null) where T : class
    {
        foreach (var doc in response.Docs)
        {
            FindHit<T> findHit = null;
            doc.Match(
                result =>
                {
                    if (result.Found)
                    {
                        var data = new DataDictionary { { ElasticDataKeys.Index, result.Index } };

                        var version = ElasticDocumentVersion.Empty;
                        if (result.PrimaryTerm.HasValue && result.SeqNo.HasValue && (result.PrimaryTerm.Value != 0 || result.SeqNo.Value != 0))
                            version = new ElasticDocumentVersion(result.PrimaryTerm.Value, result.SeqNo.Value);

                        if (result.Source is IVersioned versionedDoc)
                            versionedDoc.Version = version;

                        findHit = new FindHit<T>(result.Id, result.Source, 0, version, result.Routing, data);
                    }
                    else
                    {
                        logger?.LogDebug("MultiGet document not found: index={Index}, id={Id}", result.Index, result.Id);
                    }
                },
                error =>
                {
                    logger?.LogWarning("MultiGet document error: index={Index}, id={Id}, error={Error}", error.Index, error.Id, error.Error?.Reason);
                }
            );
            if (findHit is not null)
                yield return findHit;
        }
    }

    private static readonly long _epochTicks = new DateTimeOffset(1970, 1, 1, 0, 0, 0, 0, TimeSpan.Zero).Ticks;
    private static readonly IReadOnlyDictionary<string, object> _stringBucketData = new ReadOnlyDictionary<string, object>(new Dictionary<string, object> { { "@type", "string" } });
    private static readonly IReadOnlyDictionary<string, object> _doubleBucketData = new ReadOnlyDictionary<string, object>(new Dictionary<string, object> { { "@type", "double" } });
    private static readonly IReadOnlyDictionary<string, object> _rangeBucketData = new ReadOnlyDictionary<string, object>(new Dictionary<string, object> { { "@type", "range" } });
    private static readonly IReadOnlyDictionary<string, object> _geohashBucketData = new ReadOnlyDictionary<string, object>(new Dictionary<string, object> { { "@type", "geohash" } });

    public static IAggregate ToAggregate(this ElasticAggregations.IAggregate aggregate, string key, ITextSerializer serializer)
    {
        switch (aggregate)
        {
            case ElasticAggregations.AverageAggregate avg:
                return new ValueAggregate { Value = avg.Value, Data = avg.Meta.ToReadOnlyData<ValueAggregate>() };

            case ElasticAggregations.SumAggregate sum:
                return new ValueAggregate { Value = sum.Value, Data = sum.Meta.ToReadOnlyData<ValueAggregate>() };

            case ElasticAggregations.MinAggregate min:
                return ToValueAggregate(min.Value, min.Meta);

            case ElasticAggregations.MaxAggregate max:
                return ToValueAggregate(max.Value, max.Meta);

            case ElasticAggregations.CardinalityAggregate cardinality:
                return new ValueAggregate { Value = cardinality.Value, Data = cardinality.Meta.ToReadOnlyData<ValueAggregate>() };

            case ElasticAggregations.ValueCountAggregate valueCount:
                return new ValueAggregate { Value = valueCount.Value, Data = valueCount.Meta.ToReadOnlyData<ValueAggregate>() };

            case ElasticAggregations.ScriptedMetricAggregate scripted:
                return new ObjectValueAggregate
                {
                    Value = scripted.Value,
                    Data = scripted.Meta.ToReadOnlyData<ObjectValueAggregate>()
                };

            case ElasticAggregations.ExtendedStatsAggregate extendedStats:
                return new ExtendedStatsAggregate
                {
                    Count = extendedStats.Count,
                    Min = extendedStats.Min,
                    Max = extendedStats.Max,
                    Average = extendedStats.Avg,
                    Sum = extendedStats.Sum,
                    StdDeviation = extendedStats.StdDeviation,
                    StdDeviationBounds = extendedStats.StdDeviationBounds != null ? new StandardDeviationBounds
                    {
                        Lower = extendedStats.StdDeviationBounds.Lower,
                        Upper = extendedStats.StdDeviationBounds.Upper
                    } : null,
                    SumOfSquares = extendedStats.SumOfSquares,
                    Variance = extendedStats.Variance,
                    Data = extendedStats.Meta.ToReadOnlyData<ExtendedStatsAggregate>()
                };

            case ElasticAggregations.StatsAggregate stats:
                return new StatsAggregate
                {
                    Count = stats.Count,
                    Min = stats.Min,
                    Max = stats.Max,
                    Average = stats.Avg,
                    Sum = stats.Sum,
                    Data = stats.Meta.ToReadOnlyData<StatsAggregate>()
                };

            case ElasticAggregations.TopHitsAggregate topHits:
                var docs = topHits.Hits?.Hits?.Select(h => new ElasticLazyDocument(h, serializer)).Cast<ILazyDocument>().ToList();
                var rawHits = topHits.Hits?.Hits?
                    .Select(h => h.Source != null ? serializer.SerializeToString(h.Source) : null)
                    .Where(s => s != null)
                    .ToList();
                return new TopHitsAggregate(docs)
                {
                    Total = topHits.Hits?.Total?.Match<long>(t => t.Value, l => l) ?? 0,
                    MaxScore = topHits.Hits?.MaxScore,
                    Hits = rawHits,
                    Data = topHits.Meta.ToReadOnlyData<TopHitsAggregate>()
                };

            case ElasticAggregations.TDigestPercentilesAggregate percentiles:
                var items = percentiles.Values?.Select(item => new PercentileItem
                {
                    Percentile = item.Key,
                    Value = item.Value
                }) ?? Enumerable.Empty<PercentileItem>();
                return new PercentilesAggregate(items)
                {
                    Data = percentiles.Meta.ToReadOnlyData<PercentilesAggregate>()
                };

            case ElasticAggregations.HdrPercentilesAggregate hdrPercentiles:
                var hdrItems = hdrPercentiles.Values?.Select(item => new PercentileItem
                {
                    Percentile = item.Key,
                    Value = item.Value
                }) ?? Enumerable.Empty<PercentileItem>();
                return new PercentilesAggregate(hdrItems)
                {
                    Data = hdrPercentiles.Meta.ToReadOnlyData<PercentilesAggregate>()
                };

            case ElasticAggregations.FilterAggregate filter:
                return new SingleBucketAggregate(filter.ToAggregations(serializer))
                {
                    Data = filter.Meta.ToReadOnlyData<SingleBucketAggregate>(),
                    Total = filter.DocCount
                };

            case ElasticAggregations.GlobalAggregate globalAgg:
                return new SingleBucketAggregate(globalAgg.ToAggregations(serializer))
                {
                    Data = globalAgg.Meta.ToReadOnlyData<SingleBucketAggregate>(),
                    Total = globalAgg.DocCount
                };

            case ElasticAggregations.MissingAggregate missing:
                return new SingleBucketAggregate(missing.ToAggregations(serializer))
                {
                    Data = missing.Meta.ToReadOnlyData<SingleBucketAggregate>(),
                    Total = missing.DocCount
                };

            case ElasticAggregations.NestedAggregate nested:
                return new SingleBucketAggregate(nested.ToAggregations(serializer))
                {
                    Data = nested.Meta.ToReadOnlyData<SingleBucketAggregate>(),
                    Total = nested.DocCount
                };

            case ElasticAggregations.ReverseNestedAggregate reverseNested:
                return new SingleBucketAggregate(reverseNested.ToAggregations(serializer))
                {
                    Data = reverseNested.Meta.ToReadOnlyData<SingleBucketAggregate>(),
                    Total = reverseNested.DocCount
                };

            case ElasticAggregations.DateHistogramAggregate dateHistogram:
                return ToDateHistogramBucketAggregate(dateHistogram, serializer);

            case ElasticAggregations.StringTermsAggregate stringTerms:
                return ToTermsBucketAggregate(stringTerms, serializer);

            case ElasticAggregations.LongTermsAggregate longTerms:
                return ToTermsBucketAggregate(longTerms, serializer);

            case ElasticAggregations.DoubleTermsAggregate doubleTerms:
                return ToTermsBucketAggregate(doubleTerms, serializer);

            case ElasticAggregations.DateRangeAggregate dateRange:
                return ToRangeBucketAggregate(dateRange, serializer);

            case ElasticAggregations.RangeAggregate range:
                return ToRangeBucketAggregate(range, serializer);

            case ElasticAggregations.GeohashGridAggregate geohashGrid:
                return ToGeohashGridBucketAggregate(geohashGrid, serializer);

            default:
                return null;
        }
    }

    private static BucketAggregate ToDateHistogramBucketAggregate(ElasticAggregations.DateHistogramAggregate aggregate, ITextSerializer serializer)
    {
        var data = aggregate.Meta != null ? new Dictionary<string, object>(aggregate.Meta) : new Dictionary<string, object>();

        // Check if there's a timezone offset in the metadata
        bool hasTimezone = data.TryGetValue("@timezone", out object timezoneValue) && timezoneValue != null;

        var buckets = aggregate.Buckets.Select(b =>
        {
            // When there's a timezone, the bucket key from Elasticsearch already represents the local time boundary
            // We use Unspecified kind since the dates are adjusted for the timezone
            DateTime date = hasTimezone
                ? DateTime.SpecifyKind(b.Key.UtcDateTime, DateTimeKind.Unspecified)
                : b.Key.UtcDateTime;
            var keyAsLong = b.Key.ToUnixTimeMilliseconds();
            // Propagate timezone metadata to bucket data for round-trip serialization
            var bucketData = new Dictionary<string, object> { { "@type", "datehistogram" } };
            if (hasTimezone)
                bucketData["@timezone"] = timezoneValue;

            return (IBucket)new DateHistogramBucket(date, b.ToAggregations(serializer))
            {
                Total = b.DocCount,
                Key = keyAsLong,
                KeyAsString = b.KeyAsString ?? date.ToString("O"),
                Data = bucketData
            };
        }).ToList();

        return new BucketAggregate
        {
            Items = buckets,
            Data = new ReadOnlyDictionary<string, object>(data).ToReadOnlyData<BucketAggregate>()
        };
    }

    private static BucketAggregate ToTermsBucketAggregate(ElasticAggregations.StringTermsAggregate aggregate, ITextSerializer serializer)
    {
        var data = aggregate.Meta != null ? new Dictionary<string, object>(aggregate.Meta) : new Dictionary<string, object>();
        if (aggregate.DocCountErrorUpperBound.GetValueOrDefault() > 0)
            data.Add(nameof(aggregate.DocCountErrorUpperBound), aggregate.DocCountErrorUpperBound);
        if (aggregate.SumOtherDocCount.GetValueOrDefault() > 0)
            data.Add(nameof(aggregate.SumOtherDocCount), aggregate.SumOtherDocCount);

        var buckets = aggregate.Buckets.Select(b => (IBucket)new KeyedBucket<string>(b.ToAggregations(serializer))
        {
            Total = b.DocCount,
            Key = b.Key.ToString(),
            KeyAsString = b.Key.ToString(),
            Data = _stringBucketData
        }).ToList();

        return new BucketAggregate
        {
            Items = buckets,
            Data = new ReadOnlyDictionary<string, object>(data).ToReadOnlyData<BucketAggregate>()
        };
    }

    private static BucketAggregate ToTermsBucketAggregate(ElasticAggregations.LongTermsAggregate aggregate, ITextSerializer serializer)
    {
        var data = aggregate.Meta != null ? new Dictionary<string, object>(aggregate.Meta) : new Dictionary<string, object>();
        if (aggregate.DocCountErrorUpperBound.GetValueOrDefault() > 0)
            data.Add(nameof(aggregate.DocCountErrorUpperBound), aggregate.DocCountErrorUpperBound);
        if (aggregate.SumOtherDocCount.GetValueOrDefault() > 0)
            data.Add(nameof(aggregate.SumOtherDocCount), aggregate.SumOtherDocCount);

        var buckets = aggregate.Buckets.Select(b => (IBucket)new KeyedBucket<double>(b.ToAggregations(serializer))
        {
            Total = b.DocCount,
            Key = b.Key,
            KeyAsString = b.KeyAsString ?? b.Key.ToString(),
            Data = _doubleBucketData
        }).ToList();

        return new BucketAggregate
        {
            Items = buckets,
            Data = new ReadOnlyDictionary<string, object>(data).ToReadOnlyData<BucketAggregate>()
        };
    }

    private static BucketAggregate ToTermsBucketAggregate(ElasticAggregations.DoubleTermsAggregate aggregate, ITextSerializer serializer)
    {
        var data = aggregate.Meta != null ? new Dictionary<string, object>(aggregate.Meta) : new Dictionary<string, object>();
        if (aggregate.DocCountErrorUpperBound.GetValueOrDefault() > 0)
            data.Add(nameof(aggregate.DocCountErrorUpperBound), aggregate.DocCountErrorUpperBound);
        if (aggregate.SumOtherDocCount.GetValueOrDefault() > 0)
            data.Add(nameof(aggregate.SumOtherDocCount), aggregate.SumOtherDocCount);

        var buckets = aggregate.Buckets.Select(b => (IBucket)new KeyedBucket<double>(b.ToAggregations(serializer))
        {
            Total = b.DocCount,
            Key = b.Key,
            KeyAsString = b.KeyAsString ?? b.Key.ToString(),
            Data = _doubleBucketData
        }).ToList();

        return new BucketAggregate
        {
            Items = buckets,
            Data = new ReadOnlyDictionary<string, object>(data).ToReadOnlyData<BucketAggregate>()
        };
    }

    private static BucketAggregate ToRangeBucketAggregate(ElasticAggregations.DateRangeAggregate aggregate, ITextSerializer serializer)
    {
        var data = aggregate.Meta != null ? new Dictionary<string, object>(aggregate.Meta) : new Dictionary<string, object>();

        var buckets = aggregate.Buckets.Select(b => (IBucket)new RangeBucket(b.ToAggregations(serializer))
        {
            Total = b.DocCount,
            Key = b.Key,
            From = b.From,
            FromAsString = b.FromAsString,
            To = b.To,
            ToAsString = b.ToAsString,
            Data = _rangeBucketData
        }).ToList();

        return new BucketAggregate
        {
            Items = buckets,
            Data = new ReadOnlyDictionary<string, object>(data).ToReadOnlyData<BucketAggregate>()
        };
    }

    private static BucketAggregate ToRangeBucketAggregate(ElasticAggregations.RangeAggregate aggregate, ITextSerializer serializer)
    {
        var data = aggregate.Meta != null ? new Dictionary<string, object>(aggregate.Meta) : new Dictionary<string, object>();

        var buckets = aggregate.Buckets.Select(b => (IBucket)new RangeBucket(b.ToAggregations(serializer))
        {
            Total = b.DocCount,
            Key = b.Key,
            From = b.From,
            FromAsString = b.FromAsString,
            To = b.To,
            ToAsString = b.ToAsString,
            Data = _rangeBucketData
        }).ToList();

        return new BucketAggregate
        {
            Items = buckets,
            Data = new ReadOnlyDictionary<string, object>(data).ToReadOnlyData<BucketAggregate>()
        };
    }

    private static BucketAggregate ToGeohashGridBucketAggregate(ElasticAggregations.GeohashGridAggregate aggregate, ITextSerializer serializer)
    {
        var data = aggregate.Meta != null ? new Dictionary<string, object>(aggregate.Meta) : new Dictionary<string, object>();

        var buckets = aggregate.Buckets.Select(b => (IBucket)new KeyedBucket<string>(b.ToAggregations(serializer))
        {
            Total = b.DocCount,
            Key = b.Key,
            KeyAsString = b.Key,
            Data = _geohashBucketData
        }).ToList();

        return new BucketAggregate
        {
            Items = buckets,
            Data = new ReadOnlyDictionary<string, object>(data).ToReadOnlyData<BucketAggregate>()
        };
    }

    private static IAggregate ToValueAggregate(double? value, IReadOnlyDictionary<string, object> meta)
    {
        if (meta != null && meta.TryGetValue("@field_type", out object fieldType))
        {
            string type = fieldType?.ToString();
            if (type == "date" && value.HasValue)
            {
                var kind = DateTimeKind.Utc;
                long ticks = _epochTicks + ((long)value.Value * TimeSpan.TicksPerMillisecond);

                if (meta.TryGetValue("@timezone", out object timezoneValue) && timezoneValue != null)
                {
                    kind = DateTimeKind.Unspecified;
                    ticks -= Exceptionless.DateTimeExtensions.TimeUnit.Parse(timezoneValue.ToString()).Ticks;
                }

                return new ValueAggregate<DateTime>
                {
                    Value = GetDate(ticks, kind),
                    Data = meta.ToReadOnlyData<ValueAggregate<DateTime>>()
                };
            }
        }

        return new ValueAggregate { Value = value, Data = meta.ToReadOnlyData<ValueAggregate>() };
    }

    private static DateTime GetDate(long ticks, DateTimeKind kind)
    {
        if (ticks <= DateTime.MinValue.Ticks)
            return DateTime.MinValue;

        if (ticks >= DateTime.MaxValue.Ticks)
            return DateTime.MaxValue;

        return new DateTime(ticks, kind);
    }

    public static IReadOnlyDictionary<string, IAggregate> ToAggregations(this ElasticAggregations.AggregateDictionary aggregations, ITextSerializer serializer)
    {
        if (aggregations == null)
            return null;

        return aggregations.ToDictionary(a => a.Key, a => a.Value.ToAggregate(a.Key, serializer));
    }

    public static IReadOnlyDictionary<string, IAggregate> ToAggregations(this ElasticAggregations.DateHistogramBucket bucket, ITextSerializer serializer)
    {
        return bucket.Aggregations?.ToDictionary(a => a.Key, a => a.Value.ToAggregate(a.Key, serializer));
    }

    public static IReadOnlyDictionary<string, IAggregate> ToAggregations(this ElasticAggregations.StringTermsBucket bucket, ITextSerializer serializer)
    {
        return bucket.Aggregations?.ToDictionary(a => a.Key, a => a.Value.ToAggregate(a.Key, serializer));
    }

    public static IReadOnlyDictionary<string, IAggregate> ToAggregations(this ElasticAggregations.LongTermsBucket bucket, ITextSerializer serializer)
    {
        return bucket.Aggregations?.ToDictionary(a => a.Key, a => a.Value.ToAggregate(a.Key, serializer));
    }

    public static IReadOnlyDictionary<string, IAggregate> ToAggregations(this ElasticAggregations.DoubleTermsBucket bucket, ITextSerializer serializer)
    {
        return bucket.Aggregations?.ToDictionary(a => a.Key, a => a.Value.ToAggregate(a.Key, serializer));
    }

    public static IReadOnlyDictionary<string, IAggregate> ToAggregations(this ElasticAggregations.RangeBucket bucket, ITextSerializer serializer)
    {
        return bucket.Aggregations?.ToDictionary(a => a.Key, a => a.Value.ToAggregate(a.Key, serializer));
    }

    public static IReadOnlyDictionary<string, IAggregate> ToAggregations(this ElasticAggregations.GeohashGridBucket bucket, ITextSerializer serializer)
    {
        return bucket.Aggregations?.ToDictionary(a => a.Key, a => a.Value.ToAggregate(a.Key, serializer));
    }

    public static IReadOnlyDictionary<string, IAggregate> ToAggregations(this ElasticAggregations.FilterAggregate aggregate, ITextSerializer serializer)
    {
        return aggregate.Aggregations?.ToDictionary(a => a.Key, a => a.Value.ToAggregate(a.Key, serializer));
    }

    public static IReadOnlyDictionary<string, IAggregate> ToAggregations(this ElasticAggregations.GlobalAggregate aggregate, ITextSerializer serializer)
    {
        return aggregate.Aggregations?.ToDictionary(a => a.Key, a => a.Value.ToAggregate(a.Key, serializer));
    }

    public static IReadOnlyDictionary<string, IAggregate> ToAggregations(this ElasticAggregations.MissingAggregate aggregate, ITextSerializer serializer)
    {
        return aggregate.Aggregations?.ToDictionary(a => a.Key, a => a.Value.ToAggregate(a.Key, serializer));
    }

    public static IReadOnlyDictionary<string, IAggregate> ToAggregations(this ElasticAggregations.NestedAggregate aggregate, ITextSerializer serializer)
    {
        return aggregate.Aggregations?.ToDictionary(a => a.Key, a => a.Value.ToAggregate(a.Key, serializer));
    }

    public static IReadOnlyDictionary<string, IAggregate> ToAggregations(this ElasticAggregations.ReverseNestedAggregate aggregate, ITextSerializer serializer)
    {
        return aggregate.Aggregations?.ToDictionary(a => a.Key, a => a.Value.ToAggregate(a.Key, serializer));
    }

    public static IReadOnlyDictionary<string, IAggregate> ToAggregations<T>(this SearchResponse<T> res, ITextSerializer serializer) where T : class
    {
        return res.Aggregations.ToAggregations(serializer);
    }

    public static IReadOnlyDictionary<string, IAggregate> ToAggregations<T>(this SubmitAsyncSearchResponse<T> res, ITextSerializer serializer) where T : class
    {
        return res.Response?.Aggregations.ToAggregations(serializer);
    }

    public static IReadOnlyDictionary<string, IAggregate> ToAggregations<T>(this GetAsyncSearchResponse<T> res, ITextSerializer serializer) where T : class
    {
        return res.Response?.Aggregations.ToAggregations(serializer);
    }

    public static IReadOnlyDictionary<string, IAggregate> ToAggregations<T>(this ScrollResponse<T> res, ITextSerializer serializer) where T : class
    {
        return res.Aggregations.ToAggregations(serializer);
    }

    public static PropertiesDescriptor<T> SetupDefaults<T>(this PropertiesDescriptor<T> pd) where T : class
    {
        bool hasIdentity = typeof(IIdentity).IsAssignableFrom(typeof(T));
        bool hasDates = typeof(IHaveDates).IsAssignableFrom(typeof(T));
        bool hasCreatedDate = typeof(IHaveCreatedDate).IsAssignableFrom(typeof(T));
        bool supportsSoftDeletes = typeof(ISupportSoftDeletes).IsAssignableFrom(typeof(T));
        bool hasCustomFields = typeof(IHaveCustomFields).IsAssignableFrom(typeof(T));
        bool hasVirtualCustomFields = typeof(IHaveVirtualCustomFields).IsAssignableFrom(typeof(T));

        if (hasIdentity)
            pd.Keyword(d => ((IIdentity)d).Id);

        if (supportsSoftDeletes)
        {
            pd.Boolean(d => ((ISupportSoftDeletes)d).IsDeleted);
            pd.FieldAlias("deleted", a => a.Path(p => ((ISupportSoftDeletes)p).IsDeleted));
        }

        if (hasCreatedDate)
        {
            pd.Date(d => ((IHaveCreatedDate)d).CreatedUtc);
            pd.FieldAlias("created", a => a.Path(p => ((IHaveCreatedDate)p).CreatedUtc));
        }

        if (hasDates)
        {
            pd.Date(d => ((IHaveDates)d).UpdatedUtc);
            pd.FieldAlias("updated", a => a.Path(p => ((IHaveDates)p).UpdatedUtc));
        }

        if (hasCustomFields || hasVirtualCustomFields)
            pd.Object("idx", f => f.Dynamic(DynamicMapping.True));

        return pd;
    }
}
