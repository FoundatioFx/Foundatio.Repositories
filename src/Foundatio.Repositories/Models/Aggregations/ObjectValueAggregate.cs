using System;
using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Nodes;
using Foundatio.Serializer;

namespace Foundatio.Repositories.Models;

[DebuggerDisplay("Value: {Value}")]
public class ObjectValueAggregate : MetricAggregateBase
{
    public object? Value { get; set; }

    public T? ValueAs<T>(ITextSerializer? serializer = null)
    {
        if (Value is null)
            return default;

        if (Value is T typed)
            return typed;

        if (Value is string stringValue)
        {
            if (serializer is not null)
                return serializer.Deserialize<T>(stringValue);

            return (T?)Convert.ChangeType(stringValue, typeof(T));
        }

        if (Value is JsonNode jNode)
        {
            if (serializer is null)
                throw new InvalidOperationException($"Cannot convert {Value.GetType().Name} to {typeof(T).Name} without a serializer.");

            return serializer.Deserialize<T>(jNode.ToJsonString());
        }

        if (Value is JsonElement jElement)
        {
            if (serializer is null)
                throw new InvalidOperationException($"Cannot convert {Value.GetType().Name} to {typeof(T).Name} without a serializer.");

            return serializer.Deserialize<T>(jElement.GetRawText());
        }

        return (T?)Convert.ChangeType(Value, typeof(T));
    }
}
