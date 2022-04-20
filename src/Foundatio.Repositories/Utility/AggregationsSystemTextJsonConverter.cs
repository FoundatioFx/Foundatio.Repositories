using System;
using System.Text.Json;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories.Utility;

public class AggregationsSystemTextJsonConverter : System.Text.Json.Serialization.JsonConverter<IAggregate> {

    public override bool CanConvert(Type type) {
        return typeof(IAggregate).IsAssignableFrom(type);
    }

    public override IAggregate Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) {
        IAggregate value = null;
        
        var element = JsonElement.ParseValue(ref reader);
        string typeToken = GetTokenType(element);
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
        var serializerOptions = new JsonSerializerOptions(options) {
            Converters = { new DoubleSystemTextJsonConverter() }
        };
        
        JsonSerializer.Serialize(writer, value, value.GetType(), serializerOptions);
    }
    
    private JsonElement? GetProperty(JsonElement element, string propertyName) {
        if (element.TryGetProperty(propertyName, out var dataElement)) 
            return dataElement;
        
        if (element.TryGetProperty(propertyName.ToLower(), out dataElement)) 
            return dataElement;

        return null;
    }

    private string GetTokenType(JsonElement element) {
        var dataPropertyElement = GetProperty(element, "Data");

        if (dataPropertyElement != null && dataPropertyElement.Value.TryGetProperty("@type", out var typeElement))
            return typeElement.ToString();

        return null;
    }
}