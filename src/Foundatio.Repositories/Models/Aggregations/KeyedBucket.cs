using System.Collections.Generic;
using System.Diagnostics;
using Foundatio.Repositories.Utility;

namespace Foundatio.Repositories.Models;

[DebuggerDisplay("KeyAsString: {KeyAsString} Key: {Key} Total: {Total}")]
public class KeyedBucket<T> : BucketBase {
    public KeyedBucket() { }

    [System.Text.Json.Serialization.JsonConstructor]
    public KeyedBucket(IReadOnlyDictionary<string, IAggregate> aggregations) : base(aggregations) { }

    // NOTE: object values have been deserializad to json element (https://docs.microsoft.com/en-us/dotnet/standard/serialization/system-text-json-converters-how-to?pivots=dotnet-6-0#deserialize-inferred-types-to-object-properties)
    [System.Text.Json.Serialization.JsonConverter(typeof(ObjectToInferredTypesSystemTextJsonConverter))]
    public T Key { get; set; }
    public string KeyAsString { get; set; }
    public long? Total { get; set; }
}
