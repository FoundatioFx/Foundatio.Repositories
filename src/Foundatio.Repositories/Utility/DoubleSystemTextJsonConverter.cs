using System;
using System.Text.Json;

namespace Foundatio.Repositories.Utility;
public class DoubleSystemTextJsonConverter : System.Text.Json.Serialization.JsonConverter<double> {
    public override double Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) {
        throw new NotImplementedException();
    }

    public override bool CanConvert(Type type) {
        return typeof(double).IsAssignableFrom(type);
    }

    public override void Write(Utf8JsonWriter writer, double value, JsonSerializerOptions options) {
        writer.WriteRawValue(String.Format("{0:0.0}", value));
    }
}