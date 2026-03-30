using System.Text.Json;
using System.Text.Json.Nodes;

namespace Foundatio.Repositories.Utility;

public class MoveOperation : Operation
{
    public string FromPath { get; set; }

    public override void Write(Utf8JsonWriter writer)
    {
        writer.WriteStartObject();

        WriteOp(writer, "move");
        WritePath(writer, Path);
        WriteFromPath(writer, FromPath);

        writer.WriteEndObject();
    }

    public override void Read(JsonObject jOperation)
    {
        Path = jOperation["path"]?.GetValue<string>();
        FromPath = jOperation["from"]?.GetValue<string>();
    }
}
