using System;
using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Nodes;
using Foundatio.Serializer;

namespace Foundatio.Repositories.Models;

[DebuggerDisplay("Value: {Value}")]
public class ObjectValueAggregate : MetricAggregateBase
{
    public object Value { get; set; }

    public T ValueAs<T>(ITextSerializer serializer)
    {
        if (Value is string stringValue)
            return serializer.Deserialize<T>(stringValue);

        if (Value is JsonNode jNode)
            return serializer.Deserialize<T>(jNode.ToJsonString());

        if (Value is JsonElement jElement)
            return serializer.Deserialize<T>(jElement.GetRawText());

        return (T)Convert.ChangeType(Value, typeof(T));
    }
}
