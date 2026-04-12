using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Foundatio.Repositories.Utility;

public class TestOperation : Operation
{
    public JToken? Value { get; set; }

    public override void Write(JsonWriter writer)
    {
        writer.WriteStartObject();

        WriteOp(writer, "test");
        WritePath(writer, Path);
        if (Value is not null)
            WriteValue(writer, Value);

        writer.WriteEndObject();
    }

    public override void Read(JObject jOperation)
    {
        Path = jOperation.Value<string>("path") ?? String.Empty;
        Value = jOperation.GetValue("value");
    }
}
