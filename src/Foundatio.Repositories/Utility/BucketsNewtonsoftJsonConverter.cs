using System;
using System.Collections.Generic;
using Foundatio.Repositories.Models;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Foundatio.Repositories.Utility;

public class BucketsNewtonsoftJsonConverter : JsonConverter
{
    private static readonly long _epochTicks = new DateTimeOffset(1970, 1, 1, 0, 0, 0, 0, TimeSpan.Zero).Ticks;

    public override bool CanConvert(Type objectType)
    {
        return typeof(IBucket).IsAssignableFrom(objectType);
    }

    public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
    {
        var item = JObject.Load(reader);
        var typeToken = item.SelectToken("Data.@type") ?? item.SelectToken("data.@type");

        IBucket value = null;
        if (typeToken != null)
        {
            string type = typeToken.Value<string>();
            IReadOnlyDictionary<string, IAggregate> aggregations = null;
            var aggregationsToken = item.SelectToken("Aggregations") ?? item.SelectToken("aggregations");
            aggregations = aggregationsToken?.ToObject<IReadOnlyDictionary<string, IAggregate>>();

            switch (type)
            {
                case "datehistogram":
                    var timeZoneToken = item.SelectToken("Data.@timezone") ?? item.SelectToken("data.@timezone");
                    var kind = timeZoneToken != null ? DateTimeKind.Unspecified : DateTimeKind.Utc;
                    var key = item.SelectToken("Key") ?? item.SelectToken("key");
                    var date = new DateTime(_epochTicks + ((long)key * TimeSpan.TicksPerMillisecond), kind);

                    value = new DateHistogramBucket(date, aggregations);
                    break;
                case "range":
                    value = new RangeBucket(aggregations);
                    break;
                case "string":
                    value = new KeyedBucket<string>(aggregations);
                    break;
                case "double":
                    value = new KeyedBucket<double>(aggregations);
                    break;
                case "geohash":
                    value = new KeyedBucket<string>(aggregations);
                    break;
                case "object":
                    value = new KeyedBucket<object>(aggregations);
                    break;
            }
        }

        if (value == null)
            value = new KeyedBucket<object>();

        serializer.Populate(item.CreateReader(), value);

        return value;
    }

    public override bool CanWrite => false;

    public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
    {
        throw new NotSupportedException();
    }
}
