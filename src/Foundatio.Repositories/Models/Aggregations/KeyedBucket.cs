using System;
using System.Collections.Generic;
using System.Diagnostics;
using Foundatio.Repositories.Utility;

namespace Foundatio.Repositories.Models;

[DebuggerDisplay("KeyAsString: {KeyAsString} Key: {Key} Total: {Total}")]
public class KeyedBucket<T> : BucketBase
{
    public KeyedBucket()
    {
    }

    [System.Text.Json.Serialization.JsonConstructor]
    public KeyedBucket(IReadOnlyDictionary<string, IAggregate> aggregations) : base(aggregations)
    {
    }

    [System.Text.Json.Serialization.JsonConverter(typeof(ObjectToInferredTypesConverter))]
    public T Key { get; set; }
    public string KeyAsString { get; set; }
    public long? Total { get; set; }
}
