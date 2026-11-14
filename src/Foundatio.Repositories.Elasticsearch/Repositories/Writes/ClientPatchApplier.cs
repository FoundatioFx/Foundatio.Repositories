using System;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Elastic.Transport.Extensions;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Utility;

namespace Foundatio.Repositories.Elasticsearch;

/// <summary>
/// Applies JSON, action and (pipeline) partial patches to a document in memory, the same way for single-document and
/// batch patches.
/// </summary>
/// <remarks>
/// The configured serializer controls the document's JSON contract. The patched document keeps the metadata version
/// it was read at; the write sends it as <c>if_seq_no</c>/<c>if_primary_term</c> and the response sets the new one.
/// </remarks>
internal sealed class ClientPatchApplier<T> where T : class, new()
{
    private readonly Elastic.Transport.Serializer _serializer;
    private readonly Action<JsonNode> _applyJsonDates;
    private readonly Action<T> _setDocumentDates;

    /// <param name="serializer">The Elasticsearch source serializer.</param>
    /// <param name="applyJsonDates">Sets the updated date on a JSON-patched document (<c>ApplyDateTracking(JsonNode)</c>).</param>
    /// <param name="setDocumentDates">Sets dates on action patches and bulk JSON patches (<c>SetDocumentDates</c>).</param>
    public ClientPatchApplier(Elastic.Transport.Serializer serializer, Action<JsonNode> applyJsonDates, Action<T> setDocumentDates)
    {
        _serializer = serializer;
        _applyJsonDates = applyJsonDates;
        _setDocumentDates = setDocumentDates;
    }

    /// <summary>
    /// Returns a patched copy of <paramref name="hit"/>'s document, or <c>null</c> when an action patch made no change.
    /// </summary>
    public T? Apply(FindHit<T> hit, IPatchOperation operation, bool useDocumentDates = false)
    {
        var source = hit.Document ?? throw new DocumentException($"Document {hit.GetIndex()}/{hit.Id} returned no source");
        T? patched = operation switch
        {
            JsonPatch jsonPatch => ApplyJsonPatch(source, jsonPatch, useDocumentDates),
            ActionPatch<T> actionPatch => ApplyActionPatch(source, actionPatch),
            PartialPatch partialPatch => ApplyPartialPatch(source, partialPatch),
            _ => throw new ArgumentException("Unknown operation type", nameof(operation))
        };

        patched?.SetDocumentVersion(hit.GetElasticVersion());
        return patched;
    }

    private T ApplyJsonPatch(T source, JsonPatch operation, bool useDocumentDates)
    {
        JsonNode? target = SerializeToNode(source)
            ?? throw new DocumentException("Document JSON parsed to null; cannot apply JSON patch.");
        new JsonPatcher().Patch(ref target, operation.Patch);
        if (target is null)
            throw new DocumentException("JSON patch produced a null document.");

        if (!useDocumentDates)
            _applyJsonDates(target);

        var patched = Deserialize(target, "JSON patch produced a null document.");
        if (useDocumentDates)
            _setDocumentDates(patched);

        return patched;
    }

    private T? ApplyActionPatch(T source, ActionPatch<T> operation)
    {
        bool modified = false;
        foreach (var action in operation.Actions)
            modified |= action?.Invoke(source) ?? false;

        if (!modified)
            return null;

        _setDocumentDates(source);
        return source;
    }

    private T ApplyPartialPatch(T source, PartialPatch operation)
    {
        var sourceNode = SerializeToNode(source)
            ?? throw new DocumentException("Failed to parse source document JSON for partial merge.");
        if (sourceNode is JsonObject sourceObject && SerializeToNode(operation.Document) is JsonObject partialObject)
            MergePartialObject(sourceObject, partialObject);

        return Deserialize(sourceNode, "Partial merge produced a null document.");
    }

    private static void MergePartialObject(JsonObject source, JsonObject partial)
    {
        foreach (var property in partial)
        {
            if (source[property.Key] is JsonObject sourceObject && property.Value is JsonObject partialObject)
                MergePartialObject(sourceObject, partialObject);
            else
                source[property.Key] = property.Value?.DeepClone();
        }
    }

    private JsonNode? SerializeToNode<TValue>(TValue value)
    {
        ReadOnlySpan<byte> json = _serializer.SerializeToBytes(value);
        if (json.StartsWith(Encoding.UTF8.Preamble))
            json = json[Encoding.UTF8.Preamble.Length..];
        return JsonNode.Parse(json);
    }

    private T Deserialize(JsonNode node, string nullMessage)
    {
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream))
            node.WriteTo(writer);
        stream.Position = 0;
        return _serializer.Deserialize<T>(stream) ?? throw new DocumentException(nullMessage);
    }
}
