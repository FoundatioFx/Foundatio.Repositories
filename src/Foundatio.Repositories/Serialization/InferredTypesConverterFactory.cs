using System;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Foundatio.Repositories.Serialization;

/// <summary>
/// A <see cref="JsonConverterFactory"/> that infers CLR types from JSON tokens for generic typed properties
/// (e.g., <c>KeyedBucket&lt;T&gt;.Key</c>). Applied via <c>[JsonConverter]</c> attribute on properties
/// where the declared type is generic and the actual JSON value should be deserialized to a natural CLR type.
/// </summary>
public class InferredTypesConverterFactory : JsonConverterFactory
{
    public override bool CanConvert(Type typeToConvert) => true;

    public override JsonConverter CreateConverter(Type typeToConvert, JsonSerializerOptions options)
    {
        var converterType = typeof(InferredTypesConverter<>).MakeGenericType(typeToConvert);
        return (JsonConverter)Activator.CreateInstance(converterType);
    }

    private class InferredTypesConverter<T> : JsonConverter<T>
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
                if (result is JsonElement element)
                    return JsonSerializer.Deserialize<T>(element.GetRawText(), options)!;

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
