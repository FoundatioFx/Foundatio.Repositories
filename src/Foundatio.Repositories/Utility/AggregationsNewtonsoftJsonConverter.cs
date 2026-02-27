using System;
using System.Collections.Generic;
using Foundatio.Repositories.Models;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Foundatio.Repositories.Utility;

public class AggregationsNewtonsoftJsonConverter : JsonConverter
{
    public override bool CanConvert(Type objectType)
    {
        return typeof(IAggregate).IsAssignableFrom(objectType);
    }

    public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
    {
        var item = JObject.Load(reader);
        var typeToken = item.SelectToken("Data.@type") ?? item.SelectToken("data.@type");

        IAggregate value = null;
        if (typeToken != null)
        {
            string type = typeToken.Value<string>();
            switch (type)
            {
                case "bucket":
                    value = new BucketAggregate();
                    break;
                case "exstats":
                    value = new ExtendedStatsAggregate();
                    break;
                case "ovalue":
                    value = new ObjectValueAggregate();
                    break;
                case "percentiles":
                    var itemsToken = item.SelectToken("Items") ?? item.SelectToken("items");
                    if (itemsToken != null)
                        value = new PercentilesAggregate(itemsToken.ToObject<List<PercentileItem>>(serializer));
                    else
                        value = new PercentilesAggregate();
                    break;
                case "sbucket":
                    var aggregationsToken = item.SelectToken("Aggregations") ?? item.SelectToken("aggregations");
                    var aggregations = aggregationsToken?.ToObject<IReadOnlyDictionary<string, IAggregate>>(serializer);
                    value = new SingleBucketAggregate(aggregations);
                    break;
                case "stats":
                    value = new StatsAggregate();
                    break;
                case "tophits":
                    // TODO: Have to get all the docs as JToken and 
                    //value = new TopHitsAggregate();
                    break;
                case "value":
                    value = new ValueAggregate();
                    break;
                case "dvalue":
                    value = new ValueAggregate<DateTime>();
                    break;
            }
        }

        if (value == null)
            value = new ValueAggregate();

        serializer.Populate(item.CreateReader(), value);

        return value;
    }

    public override bool CanWrite => false;

    public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
    {
        throw new NotImplementedException();
    }
}
