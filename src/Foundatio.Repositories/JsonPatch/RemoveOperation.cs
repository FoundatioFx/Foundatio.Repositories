using System.Text.Json;
using System.Text.Json.Nodes;

namespace Foundatio.Repositories.Utility;

public class RemoveOperation : Operation
{
    public override void Write(Utf8JsonWriter writer)
    {
        writer.WriteStartObject();

        WriteOp(writer, "remove");
        WritePath(writer, Path);

        writer.WriteEndObject();
    }

    public override void Read(JsonObject jOperation)
    {
        Path = jOperation["path"]?.GetValue<string>();
    }
}
