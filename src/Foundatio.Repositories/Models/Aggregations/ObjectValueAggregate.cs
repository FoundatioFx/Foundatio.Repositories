using Newtonsoft.Json.Linq;
using System;
using System.Diagnostics;
using Foundatio.Serializer;

namespace Foundatio.Repositories.Models {
    [DebuggerDisplay("Value: {Value}")]
    public class ObjectValueAggregate : MetricAggregateBase {
        public object Value { get; set; }

        public T ValueAs<T>(ITextSerializer serializer = null) {
            if (serializer != null) {
                if (Value is string stringValue)
                    return serializer.Deserialize<T>(stringValue);
                else if (Value is JToken jTokenValue)
                    return serializer.Deserialize<T>(jTokenValue.ToString());
            }

            var jToken = Value as JToken;
            return jToken != null
                ? jToken.ToObject<T>()
                : (T)Convert.ChangeType(Value, typeof(T));
        }
    }
}
