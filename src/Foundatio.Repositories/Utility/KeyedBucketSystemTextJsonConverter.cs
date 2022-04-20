using System;
using System.Text.Json;

namespace Foundatio.Repositories.Utility;
public class KeyedBucketSystemTextJsonConverter : System.Text.Json.Serialization.JsonConverter<object> {
    public override object Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) {

        return reader.TokenType switch {
            JsonTokenType.Number => reader.GetInt64(),
            JsonTokenType.String => reader.GetString(),
            JsonTokenType.True => reader.GetBoolean(),
            JsonTokenType.False => reader.GetBoolean(),
            JsonTokenType.Null => null,

            _ => throw new NotImplementedException()
        };
    }

    public override void Write(Utf8JsonWriter writer, object value, JsonSerializerOptions options) {
        JsonSerializer.Serialize(writer, value, value.GetType(), options);
    }
}