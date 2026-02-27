using System;
using System.Collections.Generic;
using System.Text.Json;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories.Utility;

public class AggregationsSystemTextJsonConverter : System.Text.Json.Serialization.JsonConverter<IAggregate>
{
    public override bool CanConvert(Type type)
    {
        return typeof(IAggregate).IsAssignableFrom(type);
    }

    public override IAggregate Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        var element = JsonElement.ParseValue(ref reader);
        string typeToken = GetTokenType(element);

        IAggregate value = typeToken switch
        {
            "bucket" => element.Deserialize<BucketAggregate>(options),
            "exstats" => element.Deserialize<ExtendedStatsAggregate>(options),
            "ovalue" => element.Deserialize<ObjectValueAggregate>(options),
            "percentiles" => DeserializePercentiles(element, options),
            "sbucket" => element.Deserialize<SingleBucketAggregate>(options),
            "stats" => element.Deserialize<StatsAggregate>(options),
            "value" => element.Deserialize<ValueAggregate>(options),
            "dvalue" => element.Deserialize<ValueAggregate<DateTime>>(options),
            _ => null
        };

        return value ?? element.Deserialize<ValueAggregate>(options);
    }

    public override void Write(Utf8JsonWriter writer, IAggregate value, JsonSerializerOptions options)
    {
        var serializerOptions = new JsonSerializerOptions(options)
        {
            Converters = { new DoubleSystemTextJsonConverter() }
        };

        JsonSerializer.Serialize(writer, value, value.GetType(), serializerOptions);
    }

    private static PercentilesAggregate DeserializePercentiles(JsonElement element, JsonSerializerOptions options)
    {
        if (element.Deserialize<PercentilesAggregate>(options) is not { } agg)
            return new PercentilesAggregate();

        if (agg.Items is null && GetPropertyStatic(element, "Items") is { } itemsElement)
            agg.Items = itemsElement.Deserialize<IReadOnlyList<PercentileItem>>(options);

        return agg;
    }

    private static JsonElement? GetPropertyStatic(JsonElement element, string propertyName)
    {
        if (element.TryGetProperty(propertyName, out var dataElement))
            return dataElement;

        if (element.TryGetProperty(propertyName.ToLower(), out dataElement))
            return dataElement;

        return null;
    }

    private JsonElement? GetProperty(JsonElement element, string propertyName)
        => GetPropertyStatic(element, propertyName);

    private string GetTokenType(JsonElement element)
    {
        var dataPropertyElement = GetProperty(element, "Data");

        if (dataPropertyElement != null && dataPropertyElement.Value.TryGetProperty("@type", out var typeElement))
            return typeElement.ToString();

        return null;
    }
}
