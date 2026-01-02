using System;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;

namespace Foundatio.Repositories.Utility;

/// <summary>
/// JSON converter for PatchDocument using System.Text.Json.
/// Converted from Newtonsoft.Json to align with Elastic.Clients.Elasticsearch
/// which exclusively uses System.Text.Json for serialization.
/// </summary>
public class PatchDocumentConverter : JsonConverter<PatchDocument>
{
    public override PatchDocument Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (typeToConvert != typeof(PatchDocument))
            throw new ArgumentException("Object must be of type PatchDocument", nameof(typeToConvert));

        try
        {
            if (reader.TokenType == JsonTokenType.Null)
                return null;

            var node = JsonNode.Parse(ref reader);
            if (node is JsonArray array)
            {
                return PatchDocument.Load(array);
            }

            throw new ArgumentException("Invalid patch document: expected array");
        }
        catch (Exception ex)
        {
            throw new ArgumentException("Invalid patch document: " + ex.Message);
        }
    }

    public override void Write(Utf8JsonWriter writer, PatchDocument value, JsonSerializerOptions options)
    {
        if (value == null)
        {
            writer.WriteNullValue();
            return;
        }

        writer.WriteStartArray();
        foreach (var op in value.Operations)
            op.Write(writer);
        writer.WriteEndArray();
    }
}
