using System;
using System.Text.Json;

namespace Foundatio.Repositories.Utility;

// NOTE: This fixes an issue where doubles were converted to integers (https://github.com/dotnet/runtime/issues/35195)
public class DoubleSystemTextJsonConverter : System.Text.Json.Serialization.JsonConverter<double>
{
    public override double Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        throw new NotImplementedException();
    }

    public override bool CanConvert(Type type)
    {
        return typeof(double).IsAssignableFrom(type);
    }

    public override void Write(Utf8JsonWriter writer, double value, JsonSerializerOptions options)
    {
        writer.WriteRawValue($"{value:0.0}");
    }
}
