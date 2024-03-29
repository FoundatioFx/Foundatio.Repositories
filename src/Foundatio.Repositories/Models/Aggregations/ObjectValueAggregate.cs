﻿using System;
using System.Diagnostics;
using Foundatio.Serializer;
using Newtonsoft.Json.Linq;

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
            else if (Value is JToken jTokenValue)
                return serializer.Deserialize<T>(jTokenValue.ToString());
        }

        return Value is JToken jToken
            ? jToken.ToObject<T>()
            : (T)Convert.ChangeType(Value, typeof(T));
    }
}
