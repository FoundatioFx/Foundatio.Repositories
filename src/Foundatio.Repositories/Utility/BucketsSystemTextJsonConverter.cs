using System;
using System.Text.Json;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories.Utility;

public class BucketsSystemTextJsonConverter : System.Text.Json.Serialization.JsonConverter<IBucket>
{
    private static readonly long _epochTicks = new DateTimeOffset(1970, 1, 1, 0, 0, 0, 0, TimeSpan.Zero).Ticks;

    public override bool CanConvert(Type type)
    {
        return typeof(IBucket).IsAssignableFrom(type);
    }

    public override IBucket Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        IBucket value = null;

        var element = JsonElement.ParseValue(ref reader);
        string typeToken = GetDataToken(element, "@type");
        if (typeToken != null)
        {
            switch (typeToken)
            {
                case "datehistogram":
                    // First deserialize as KeyedBucket<double> to get all properties
                    var tempBucket = element.Deserialize<KeyedBucket<double>>(options);

                    // Calculate date from Key (epoch milliseconds)
                    string timeZoneToken = GetDataToken(element, "@timezone");
                    var kind = timeZoneToken != null ? DateTimeKind.Unspecified : DateTimeKind.Utc;
                    long keyAsLong = (long)tempBucket.Key;
                    var date = new DateTime(_epochTicks + (keyAsLong * TimeSpan.TicksPerMillisecond), kind);

                    // Create DateHistogramBucket with all properties from the deserialized bucket
                    var bucket = new DateHistogramBucket(date, tempBucket.Aggregations)
                    {
                        Key = tempBucket.Key,
                        KeyAsString = tempBucket.KeyAsString,
                        Total = tempBucket.Total,
                        Data = tempBucket.Data
                    };
                    value = bucket;
                    break;
                case "range":
                    value = element.Deserialize<RangeBucket>(options);
                    break;
                case "string":
                    value = element.Deserialize<KeyedBucket<string>>(options);
                    break;
                case "double":
                    value = element.Deserialize<KeyedBucket<double>>(options);
                    break;
                case "object":
                    value = element.Deserialize<KeyedBucket<object>>(options);
                    break;
            }
        }

        if (value is null)
            value = element.Deserialize<KeyedBucket<object>>(options);

        return value;
    }

    public override void Write(Utf8JsonWriter writer, IBucket value, JsonSerializerOptions options)
    {
        JsonSerializer.Serialize(writer, value, value.GetType(), options);
    }

    private JsonElement? GetProperty(JsonElement element, string propertyName)
    {
        if (element.TryGetProperty(propertyName, out var dataElement))
            return dataElement;

        if (element.TryGetProperty(propertyName.ToLower(), out dataElement))
            return dataElement;

        return null;
    }

    private string GetDataToken(JsonElement element, string key)
    {
        var dataPropertyElement = GetProperty(element, "Data");
        if (dataPropertyElement != null && dataPropertyElement.Value.TryGetProperty(key, out var typeElement))
            return typeElement.ToString();

        return null;
    }
}
