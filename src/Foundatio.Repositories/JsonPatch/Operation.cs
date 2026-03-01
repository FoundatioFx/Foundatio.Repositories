using System;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Foundatio.Repositories.Utility;

/// <summary>
/// Base class for JSON Patch operations (RFC 6902).
/// Converted from Newtonsoft.Json to System.Text.Json to align with Elastic.Clients.Elasticsearch
/// which exclusively uses System.Text.Json for serialization.
/// </summary>
public abstract class Operation
{
    public string Path { get; set; }

    public abstract void Write(Utf8JsonWriter writer);

    protected static void WriteOp(Utf8JsonWriter writer, string op)
    {
        writer.WriteString("op", op);
    }

    protected static void WritePath(Utf8JsonWriter writer, string path)
    {
        writer.WriteString("path", path);
    }

    protected static void WriteFromPath(Utf8JsonWriter writer, string path)
    {
        writer.WriteString("from", path);
    }

    protected static void WriteValue(Utf8JsonWriter writer, JsonNode value)
    {
        writer.WritePropertyName("value");
        if (value == null)
            writer.WriteNullValue();
        else
            value.WriteTo(writer);
    }

    public abstract void Read(JsonObject jOperation);

    public static Operation Parse(string json)
    {
        return Build(JsonNode.Parse(json)?.AsObject());
    }

    public static Operation Build(JsonObject jOperation)
    {
        ArgumentNullException.ThrowIfNull(jOperation);

        var opName = jOperation["op"]?.GetValue<string>();
        if (String.IsNullOrWhiteSpace(opName))
            throw new ArgumentException("The JSON patch operation must contain a non-empty 'op' property.", nameof(jOperation));

        var op = PatchDocument.CreateOperation(opName)
            ?? throw new ArgumentException($"Unsupported JSON patch operation type '{opName}'.", nameof(jOperation));

        op.Read(jOperation);
        return op;
    }
}
