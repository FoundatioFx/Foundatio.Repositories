using System;
using System.Globalization;
using System.Text.Json;

namespace Foundatio.Repositories.Serialization;

/// <summary>
/// Preserves decimal points on whole-number doubles during JSON serialization.
/// Without this, <c>1.0</c> round-trips as <c>1</c>, losing the floating-point representation.
/// </summary>
/// <seealso href="https://github.com/dotnet/runtime/issues/35195"/>
public class DoubleSystemTextJsonConverter : System.Text.Json.Serialization.JsonConverter<double>
{
    public override double Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        return reader.GetDouble();
    }

    public override bool CanConvert(Type type)
    {
        return typeof(double).IsAssignableFrom(type);
    }

    public override void Write(Utf8JsonWriter writer, double value, JsonSerializerOptions options)
    {
        if (double.IsNaN(value) || double.IsInfinity(value))
        {
            writer.WriteRawValue("0.0");
            return;
        }

        if (value != Math.Truncate(value))
        {
            writer.WriteNumberValue(value);
            return;
        }

        string text = value.ToString("R", CultureInfo.InvariantCulture);
        if (!text.Contains('.') && !text.Contains('E') && !text.Contains('e'))
            text += ".0";

        writer.WriteRawValue(text);
    }
}
