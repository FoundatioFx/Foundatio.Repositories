using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Foundatio.Repositories.Utility;

public abstract class Operation
{
    public string Path { get; set; }

    public abstract void Write(JsonWriter writer);

    protected static void WriteOp(JsonWriter writer, string op)
    {
        writer.WritePropertyName("op");
        writer.WriteValue(op);
    }

    protected static void WritePath(JsonWriter writer, string path)
    {
        writer.WritePropertyName("path");
        writer.WriteValue(path);
    }

    protected static void WriteFromPath(JsonWriter writer, string path)
    {
        writer.WritePropertyName("from");
        writer.WriteValue(path);
    }

    protected static void WriteValue(JsonWriter writer, JToken value)
    {
        writer.WritePropertyName("value");
        value.WriteTo(writer);
    }

    public abstract void Read(JObject jOperation);

    public static Operation Parse(string json)
    {
        return Build(JObject.Parse(json));
    }

    public static Operation Build(JObject jOperation)
    {
        ArgumentNullException.ThrowIfNull(jOperation);

        var opName = (string)jOperation["op"];
        if (String.IsNullOrWhiteSpace(opName))
            throw new ArgumentException("The JSON patch operation must contain a non-empty 'op' property.", nameof(jOperation));

        var op = PatchDocument.CreateOperation(opName)
            ?? throw new ArgumentException($"Unsupported JSON patch operation type '{opName}'.", nameof(jOperation));

        op.Read(jOperation);
        return op;
    }
}
