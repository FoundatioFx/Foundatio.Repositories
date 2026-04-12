using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Foundatio.Repositories.Utility;

public class CopyOperation : Operation
{
    public string? FromPath { get; set; }

    public override void Write(JsonWriter writer)
    {
        writer.WriteStartObject();

        WriteOp(writer, "copy");
        WritePath(writer, Path ?? String.Empty);
        WriteFromPath(writer, FromPath ?? String.Empty);

        writer.WriteEndObject();
    }

    public override void Read(JObject jOperation)
    {
        Path = jOperation.Value<string>("path");
        FromPath = jOperation.Value<string>("from");
    }
}
