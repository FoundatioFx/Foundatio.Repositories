using System.Collections.Generic;
using System.Diagnostics;
using Foundatio.Repositories.Utility;

namespace Foundatio.Repositories.Models;

[DebuggerDisplay("KeyAsString: {KeyAsString} Key: {Key} Total: {Total}")]
public class KeyedBucket<T> : BucketBase {
    public KeyedBucket() { }
    public KeyedBucket(IDictionary<string, IAggregate> aggregations) : base(aggregations) { }

    [System.Text.Json.Serialization.JsonConverter(typeof(KeyedBucketSystemTextJsonConverter))]
    public T Key { get; set; }
    public string KeyAsString { get; set; }
    public long? Total { get; set; }
}
