using System;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Foundatio.Repositories.Utility;

public class ObjectToInferredTypesConverter : JsonConverterFactory
{
    public override bool CanConvert(Type typeToConvert) => true;

    public override JsonConverter CreateConverter(Type typeToConvert, JsonSerializerOptions options)
    {
        var converterType = typeof(ObjectToInferredTypesConverterInner<>).MakeGenericType(typeToConvert);
        return (JsonConverter)Activator.CreateInstance(converterType);
    }

    private class ObjectToInferredTypesConverterInner<T> : JsonConverter<T>
    {
        public override T Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            object result = reader.TokenType switch
            {
                JsonTokenType.Number when reader.TryGetInt64(out long l) => l,
                JsonTokenType.Number => reader.GetDouble(),
                JsonTokenType.String when reader.TryGetDateTime(out DateTime datetime) => datetime,
                JsonTokenType.String => reader.GetString()!,
                JsonTokenType.True => true,
                JsonTokenType.False => false,
                JsonTokenType.Null => null!,
                _ => JsonDocument.ParseValue(ref reader).RootElement.Clone()
            };

            if (result == null)
                return default!;

            if (result is T typedResult)
                return typedResult;

            try
            {
                // Special case for JsonElement
                if (result is JsonElement element)
                {
                    return JsonSerializer.Deserialize<T>(element.GetRawText(), options)!;
                }

                return (T)Convert.ChangeType(result, typeof(T));
            }
            catch (Exception ex)
            {
                throw new JsonException($"Cannot convert {result} to type {typeof(T)}: {ex.Message}", ex);
            }
        }

        public override void Write(Utf8JsonWriter writer, T value, JsonSerializerOptions options)
        {
            JsonSerializer.Serialize(writer, value, typeof(T), options);
        }
    }
}
