using System;
using System.Collections.Generic;
using System.Text.Json;
using Foundatio.Repositories.Models;
using Newtonsoft.Json.Linq;

namespace Foundatio.Repositories.Utility;

public class BucketsSystemTextJsonConverter : System.Text.Json.Serialization.JsonConverter<IBucket> {
    public override bool CanConvert(Type type) {
        return typeof(IBucket).IsAssignableFrom(type);
    }
    private static readonly long _epochTicks = new DateTimeOffset(1970, 1, 1, 0, 0, 0, 0, TimeSpan.Zero).Ticks;

    public override IBucket Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) {
        var element = JsonElement.ParseValue(ref reader);
        string typeToken = GetDataToken(element, "@type");
        IBucket value = null;
        if (typeToken != null) {

            IReadOnlyDictionary<string, IAggregate> aggregations = null;
            var aggregationsElement = GetProperty(element, "Aggregations");
            aggregations = aggregationsElement?.Deserialize<IReadOnlyDictionary<string, IAggregate>>(options);

            switch (typeToken) {
                case "datehistogram":
                    var timeZoneToken = GetDataToken(element, "@timezone");
                    var kind = timeZoneToken != null ? DateTimeKind.Unspecified : DateTimeKind.Utc;
                    var key = GetProperty(element, "Key")?.GetInt64() ?? throw new InvalidOperationException();
                    var date = new DateTime(_epochTicks + (key * TimeSpan.TicksPerMillisecond), kind);
                    value = new DateHistogramBucket(date, aggregations);
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

    public override void Write(Utf8JsonWriter writer, IBucket value, JsonSerializerOptions options) {
        JsonSerializer.Serialize(writer, value, value.GetType(), options);
    }

    private JsonElement? GetProperty(JsonElement element, string propertyName) {
        if (element.TryGetProperty(propertyName, out var dataElement)) {
            return dataElement;
        }
        if (element.TryGetProperty(propertyName.ToLower(), out dataElement)) {
            return dataElement;
        }

        return null;
    }

    private string GetDataToken(JsonElement element, string key) {
        var dataPropertyElement = GetProperty(element, "Data");

        if (dataPropertyElement != null && dataPropertyElement.Value.TryGetProperty(key, out var typeElement)) {
            return typeElement.ToString();
        }

        return null;
    }
}