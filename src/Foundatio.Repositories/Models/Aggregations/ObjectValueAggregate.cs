using Newtonsoft.Json.Linq;
using System;
using System.Diagnostics;

namespace Foundatio.Repositories.Models {
    [DebuggerDisplay("Value: {Value}")]
    public class ObjectValueAggregate : MetricAggregateBase {
        public object Value { get; set; }

        public T ValueAs<T>() {
            var jToken = Value as JToken;
            return jToken != null
                ? jToken.ToObject<T>()
                : (T)Convert.ChangeType(Value, typeof(T));
        }
    }
}
