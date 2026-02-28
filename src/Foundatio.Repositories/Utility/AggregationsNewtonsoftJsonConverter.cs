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
        string type = typeToken?.Value<string>();

        IAggregate value = type switch
        {
            "bucket" => new BucketAggregate(),
            "exstats" => new ExtendedStatsAggregate(),
            "ovalue" => new ObjectValueAggregate(),
            "percentiles" => DeserializePercentiles(item, serializer),
            "sbucket" => DeserializeSingleBucket(item, serializer),
            "stats" => new StatsAggregate(),
            "tophits" => new TopHitsAggregate(),
            "value" => new ValueAggregate(),
            "dvalue" => new ValueAggregate<DateTime>(),
            _ => null
        };

        value ??= new ValueAggregate();

        serializer.Populate(item.CreateReader(), value);

        return value;
    }

    private static PercentilesAggregate DeserializePercentiles(JObject item, JsonSerializer serializer)
    {
        if ((item.SelectToken("Items") ?? item.SelectToken("items")) is { } itemsToken)
            return new PercentilesAggregate(itemsToken.ToObject<IReadOnlyList<PercentileItem>>(serializer));

        return new PercentilesAggregate();
    }

    private static SingleBucketAggregate DeserializeSingleBucket(JObject item, JsonSerializer serializer)
    {
        var aggregationsToken = item.SelectToken("Aggregations") ?? item.SelectToken("aggregations");
        var aggregations = aggregationsToken?.ToObject<IReadOnlyDictionary<string, IAggregate>>(serializer);
        return new SingleBucketAggregate(aggregations);
    }

    public override bool CanWrite => false;

    public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
    {
        throw new NotImplementedException();
    }
}
