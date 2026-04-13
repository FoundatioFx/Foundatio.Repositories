using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Foundatio.Repositories.Serialization;

/// <summary>
/// A System.Text.Json converter that deserializes <c>object</c>-typed properties
/// into appropriate .NET types instead of the default <see cref="JsonElement"/> behavior.
/// </summary>
/// <remarks>
/// <para>
/// By default, System.Text.Json deserializes properties typed as <c>object</c> into <see cref="JsonElement"/>,
/// which requires additional handling to extract values. This converter infers the actual type from the JSON
/// token and deserializes directly to native .NET types:
/// </para>
/// <list type="bullet">
///   <item><description><c>true</c>/<c>false</c> → <see cref="bool"/></description></item>
///   <item><description>Numbers → <see cref="long"/> for integers, <see cref="double"/> for floats</description></item>
///   <item><description>Strings with ISO 8601 datetime format (containing 'T') → <see cref="DateTimeOffset"/> or <see cref="DateTime"/></description></item>
///   <item><description>Other strings → <see cref="string"/></description></item>
///   <item><description><c>null</c> → <c>null</c></description></item>
///   <item><description>Objects → <see cref="Dictionary{TKey,TValue}"/> with <see cref="StringComparer.OrdinalIgnoreCase"/></description></item>
///   <item><description>Arrays → <see cref="List{T}"/> of <see cref="object"/></description></item>
/// </list>
/// </remarks>
/// <seealso href="https://learn.microsoft.com/en-us/dotnet/standard/serialization/system-text-json/converters-how-to#deserialize-inferred-types-to-object-properties"/>
public sealed class ObjectToInferredTypesConverter : JsonConverter<object>
{
    public override object? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        return reader.TokenType switch
        {
            JsonTokenType.True => true,
            JsonTokenType.False => false,
            JsonTokenType.Number => ReadNumber(ref reader),
            JsonTokenType.String => ReadString(ref reader),
            JsonTokenType.Null => null,
            JsonTokenType.StartObject => ReadObject(ref reader, options),
            JsonTokenType.StartArray => ReadArray(ref reader, options),
            _ => JsonDocument.ParseValue(ref reader).RootElement.Clone()
        };
    }

    public override void Write(Utf8JsonWriter writer, object value, JsonSerializerOptions options)
    {
        if (value is null)
        {
            writer.WriteNullValue();
            return;
        }

        if (value is JsonElement element)
        {
            element.WriteTo(writer);
            return;
        }

        JsonSerializer.Serialize(writer, value, value.GetType(), options);
    }

    /// <summary>
    /// Reads a JSON number, preserving the original representation (integer vs floating-point).
    /// Checks the raw JSON bytes so that <c>0.0</c> stays as <see cref="double"/> instead of becoming <c>0L</c>.
    /// </summary>
    private static object ReadNumber(ref Utf8JsonReader reader)
    {
        ReadOnlySpan<byte> rawValue = reader.HasValueSequence
            ? reader.ValueSequence.ToArray()
            : reader.ValueSpan;

        if (rawValue.Contains((byte)'.') || rawValue.Contains((byte)'e') || rawValue.Contains((byte)'E'))
            return reader.GetDouble();

        if (reader.TryGetInt64(out long l))
            return l;

        return reader.GetDouble();
    }

    private static object ReadString(ref Utf8JsonReader reader)
    {
        // Only attempt date parsing for strings that contain 'T', which is required
        // by ISO 8601 datetime format (e.g. "2025-01-01T10:30:00Z"). Date-only strings
        // like "2025-01-01" also satisfy TryGetDateTimeOffset but should remain as strings
        // since they are typically identifiers, labels, or display values — not timestamps.
        ReadOnlySpan<byte> raw = reader.HasValueSequence
            ? reader.ValueSequence.ToArray()
            : reader.ValueSpan;

        if (raw.Contains((byte)'T'))
        {
            if (reader.TryGetDateTimeOffset(out DateTimeOffset dateTimeOffset))
                return dateTimeOffset;

            if (reader.TryGetDateTime(out DateTime dt))
                return dt;
        }

        return reader.GetString()!;
    }

    private Dictionary<string, object> ReadObject(ref Utf8JsonReader reader, JsonSerializerOptions options)
    {
        var dictionary = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);

        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndObject)
                return dictionary;

            if (reader.TokenType != JsonTokenType.PropertyName)
                continue;

            string propertyName = reader.GetString() ?? string.Empty;

            if (!reader.Read())
                continue;

            dictionary[propertyName] = ReadValue(ref reader, options)!;
        }

        return dictionary;
    }

    private List<object> ReadArray(ref Utf8JsonReader reader, JsonSerializerOptions options)
    {
        var list = new List<object>();

        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndArray)
                return list;

            list.Add(ReadValue(ref reader, options)!);
        }

        return list;
    }

    private object? ReadValue(ref Utf8JsonReader reader, JsonSerializerOptions options)
    {
        return reader.TokenType switch
        {
            JsonTokenType.True => true,
            JsonTokenType.False => false,
            JsonTokenType.Number => ReadNumber(ref reader),
            JsonTokenType.String => ReadString(ref reader),
            JsonTokenType.Null => null,
            JsonTokenType.StartObject => ReadObject(ref reader, options),
            JsonTokenType.StartArray => ReadArray(ref reader, options),
            _ => JsonDocument.ParseValue(ref reader).RootElement.Clone()
        };
    }
}
