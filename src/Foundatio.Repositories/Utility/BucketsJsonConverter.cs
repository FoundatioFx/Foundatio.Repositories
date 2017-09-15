using System;
using Foundatio.Repositories.Models;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Foundatio.Repositories.Utility {
    public class BucketsJsonConverter : JsonConverter {
        public override bool CanConvert(Type objectType) {
            return typeof(IBucket).IsAssignableFrom(objectType);
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer) {
            var item = JObject.Load(reader);
            var typeToken = item.SelectToken("Data.@type") ?? item.SelectToken("data.@type");

            IBucket value = null;
            if (typeToken != null) {
                string type = typeToken.Value<string>();
                switch (type) {
                    case "datehistogram":
                        value = new DateHistogramBucket();
                        break;
                    case "range":
                        value = new RangeBucket();
                        break;
                    case "string":
                        value = new KeyedBucket<string>();
                        break;
                    case "double":
                        value = new KeyedBucket<double>();
                        break;
                    case "object":
                        value = new KeyedBucket<object>();
                        break;
                }
            }

            if (value == null)
                value = new KeyedBucket<object>();

            serializer.Populate(item.CreateReader(), value);
            return value;
        }

        public override bool CanWrite => false;

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer) {
            throw new NotImplementedException();
        }
    }
}