using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;

namespace Foundatio.Repositories.Utility;

/// <summary>
/// Represents a JSON Patch document (RFC 6902).
/// Converted from Newtonsoft.Json to System.Text.Json to align with Elastic.Clients.Elasticsearch
/// which exclusively uses System.Text.Json for serialization.
/// </summary>
[JsonConverter(typeof(PatchDocumentConverter))]
public class PatchDocument
{
    private readonly List<Operation> _operations = new();

    public PatchDocument(params Operation[] operations)
    {
        foreach (var operation in operations)
        {
            AddOperation(operation);
        }
    }

    public List<Operation> Operations => _operations;

    public void Add(string path, JsonNode value)
    {
        Operations.Add(new AddOperation { Path = path, Value = value });
    }

    public void Add(string path, string value) => Add(path, JsonValue.Create(value));
    public void Add(string path, int value) => Add(path, JsonValue.Create(value));
    public void Add(string path, long value) => Add(path, JsonValue.Create(value));
    public void Add(string path, double value) => Add(path, JsonValue.Create(value));
    public void Add(string path, bool value) => Add(path, JsonValue.Create(value));

    public void Replace(string path, JsonNode value)
    {
        Operations.Add(new ReplaceOperation { Path = path, Value = value });
    }

    public void Replace(string path, string value) => Replace(path, JsonValue.Create(value));
    public void Replace(string path, int value) => Replace(path, JsonValue.Create(value));
    public void Replace(string path, long value) => Replace(path, JsonValue.Create(value));
    public void Replace(string path, double value) => Replace(path, JsonValue.Create(value));
    public void Replace(string path, bool value) => Replace(path, JsonValue.Create(value));

    public void Remove(string path)
    {
        Operations.Add(new RemoveOperation { Path = path });
    }

    public void AddOperation(Operation operation)
    {
        Operations.Add(operation);
    }

    public static PatchDocument Load(Stream document)
    {
        using var reader = new StreamReader(document, leaveOpen: true);

        return Parse(reader.ReadToEnd());
    }

    public static PatchDocument Load(JsonArray document)
    {
        var root = new PatchDocument();

        if (document == null)
            return root;

        foreach (var item in document)
        {
            if (item is not JsonObject jOperation)
                throw new JsonException($"Invalid patch operation: expected a JSON object but found {item?.GetValueKind().ToString() ?? "null"}");

            var op = Operation.Build(jOperation);
            root.AddOperation(op);
        }

        return root;
    }

    public static PatchDocument Parse(string jsondocument)
    {
        var root = JsonNode.Parse(jsondocument) as JsonArray;

        return Load(root);
    }

    public static Operation CreateOperation(string op)
    {
        switch (op)
        {
            case "add":
                return new AddOperation();
            case "copy":
                return new CopyOperation();
            case "move":
                return new MoveOperation();
            case "remove":
                return new RemoveOperation();
            case "replace":
                return new ReplaceOperation();
            case "test":
                return new TestOperation();
        }
        return null;
    }

    public MemoryStream ToStream()
    {
        var stream = new MemoryStream();
        CopyToStream(stream);
        stream.Flush();
        stream.Position = 0;
        return stream;
    }

    public void CopyToStream(Stream stream, bool indented = true)
    {
        using var writer = new Utf8JsonWriter(stream, new JsonWriterOptions { Indented = indented });

        writer.WriteStartArray();

        foreach (var operation in Operations)
            operation.Write(writer);

        writer.WriteEndArray();

        writer.Flush();
    }

    public override string ToString()
    {
        return ToString(indented: true);
    }

    public string ToString(bool indented)
    {
        using var ms = new MemoryStream();
        CopyToStream(ms, indented);
        ms.Position = 0;
        using StreamReader reader = new StreamReader(ms, Encoding.UTF8);
        return reader.ReadToEnd();
    }
}
