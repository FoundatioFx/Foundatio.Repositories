using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Foundatio.Repositories.Utility;

public class CopyOperation : Operation
{
    public string FromPath { get; set; } = String.Empty;

    public override void Write(JsonWriter writer)
    {
        writer.WriteStartObject();

        WriteOp(writer, "copy");
        WritePath(writer, Path);
        WriteFromPath(writer, FromPath);

        writer.WriteEndObject();
    }

    public override void Read(JObject jOperation)
    {
        Path = jOperation.Value<string>("path") ?? String.Empty;
        FromPath = jOperation.Value<string>("from") ?? String.Empty;
    }
}
