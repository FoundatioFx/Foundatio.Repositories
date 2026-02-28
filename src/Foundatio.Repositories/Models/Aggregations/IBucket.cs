using System.Collections.Generic;
using Foundatio.Repositories.Utility;

namespace Foundatio.Repositories.Models;

/// <summary>
/// Represents a bucket in a bucket aggregation result.
/// </summary>
/// <remarks>
/// Buckets group documents based on field values, ranges, or other criteria. Each bucket
/// contains a subset of documents and can include nested aggregations.
/// </remarks>
[Newtonsoft.Json.JsonConverter(typeof(BucketsNewtonsoftJsonConverter))]
[System.Text.Json.Serialization.JsonConverter(typeof(BucketsSystemTextJsonConverter))]
public interface IBucket
{
    /// <summary>
    /// Gets or sets the bucket data, including document count and nested aggregations.
    /// </summary>
    IReadOnlyDictionary<string, object> Data { get; set; }
}
