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

    public T ValueAs<T>(ITextSerializer serializer = null)
    {
        if (Value is string stringValue && serializer is not null)
            return serializer.Deserialize<T>(stringValue);

        if (Value is JsonNode jNode)
        {
            if (serializer is not null)
                return serializer.Deserialize<T>(jNode.ToJsonString());
            return jNode.Deserialize<T>();
        }

        if (Value is JsonElement jElement)
        {
            if (serializer is not null)
                return serializer.Deserialize<T>(jElement.GetRawText());
            return jElement.Deserialize<T>();
        }

        return (T)Convert.ChangeType(Value, typeof(T));
    }
}
