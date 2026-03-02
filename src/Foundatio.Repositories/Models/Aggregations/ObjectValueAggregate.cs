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
        if (serializer != null)
        {
            if (Value is string stringValue)
                return serializer.Deserialize<T>(stringValue);
            else if (Value is JsonNode jsonNodeValue)
                return serializer.Deserialize<T>(jsonNodeValue.ToJsonString());
            else if (Value is JsonElement jsonElementValue)
                return serializer.Deserialize<T>(jsonElementValue.GetRawText());
        }

        // Handle System.Text.Json types (used by Elastic.Clients.Elasticsearch)
        if (Value is JsonNode jNode)
            return jNode.Deserialize<T>();
        if (Value is JsonElement jElement)
            return jElement.Deserialize<T>();

        return (T)Convert.ChangeType(Value, typeof(T));
    }
}
