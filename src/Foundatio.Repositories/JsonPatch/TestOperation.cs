using System.Text.Json;
using System.Text.Json.Nodes;

namespace Foundatio.Repositories.Utility;

public class TestOperation : Operation
{
    public JsonNode Value { get; set; }

    public override void Write(Utf8JsonWriter writer)
    {
        writer.WriteStartObject();

        WriteOp(writer, "test");
        WritePath(writer, Path);
        WriteValue(writer, Value);

        writer.WriteEndObject();
    }

    public override void Read(JsonObject jOperation)
    {
        Path = jOperation["path"]?.GetValue<string>();
        Value = jOperation["value"]?.DeepClone();
    }
}
