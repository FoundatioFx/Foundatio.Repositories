using System;
using System.Text.Json;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories.Utility;

public class AggregationsSystemTextJsonConverter : System.Text.Json.Serialization.JsonConverter<IAggregate> {

    public override bool CanConvert(Type type) {
        return typeof(IAggregate).IsAssignableFrom(type);
    }
    public override IAggregate Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) {
        var element = JsonElement.ParseValue(ref reader);
        string typeToken = GetTokenType(element);
        IAggregate value = null;
        if (typeToken != null) {
            switch (typeToken) {
                case "bucket":
                    value = element.Deserialize<BucketAggregate>(options);
                    break;
                case "exstats":
                    value = element.Deserialize<ExtendedStatsAggregate>(options);
                    break;
                case "ovalue":
                    value = element.Deserialize<ObjectValueAggregate>(options);
                    break;
                case "percentiles":
                    value = element.Deserialize<PercentilesAggregate>(options);
                    break;
                case "sbucket":
                    value = element.Deserialize<SingleBucketAggregate>(options);
                    break;
                case "stats":
                    value = element.Deserialize<StatsAggregate>(options);
                    break;
                case "tophits":
                    // TODO: Have to get all the docs as JToken and 
                    //value = new TopHitsAggregate();
                    break;
                case "value":
                    value = element.Deserialize<ValueAggregate>(options);
                    break;
                case "dvalue":
                    value = element.Deserialize<ValueAggregate<DateTime>>(options);
                    break;
            }
        }
        if (value is null)
            value = element.Deserialize<ValueAggregate>(options);
        return value;
    }

    public override void Write(Utf8JsonWriter writer, IAggregate value, JsonSerializerOptions options) {

        JsonSerializerOptions opt = new JsonSerializerOptions(options) { Converters = { new DoubleJsonConverter() } };
        JsonSerializer.Serialize(writer, value, value.GetType(), opt);
    }
    private string GetTokenType(JsonElement element) {

        if (element.TryGetProperty("Data", out var dataElement)) {
            if (dataElement.TryGetProperty("@type", out var typeElement)) {
                return typeElement.ToString();
            }
            return null;
        }
        if (element.TryGetProperty("data", out dataElement)) {
            if (dataElement.TryGetProperty("@type", out var typeElement)) {
                return typeElement.ToString();
            }
            return null;
        }
        return null;
    }
}