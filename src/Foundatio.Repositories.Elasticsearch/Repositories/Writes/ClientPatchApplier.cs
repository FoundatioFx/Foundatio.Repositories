using System;
using System.IO;
using System.Text;
using System.Text.Json.Nodes;
using Elastic.Transport;
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
/// <see cref="IVersioned.Version"/> is not part of <c>_source</c>, so a JSON patch operation that targets <c>/version</c>
/// is rejected with an <see cref="ArgumentException"/> instead of silently having no effect. The patched document keeps
/// the version it was read at; the write sends it as <c>if_seq_no</c>/<c>if_primary_term</c> and the response sets the new one.
/// </remarks>
internal sealed class ClientPatchApplier<T> where T : class, new()
{
    private readonly Elastic.Transport.Serializer _serializer;
    private readonly Action<JsonNode> _applyJsonDates;
    private readonly Action<T> _setDocumentDates;

    /// <param name="serializer">The Elasticsearch source serializer.</param>
    /// <param name="applyJsonDates">Sets the updated date on a JSON-patched document (<c>ApplyDateTracking(JsonNode)</c>).</param>
    /// <param name="setDocumentDates">Sets dates on an action-patched document (<c>SetDocumentDates</c>).</param>
    public ClientPatchApplier(Elastic.Transport.Serializer serializer, Action<JsonNode> applyJsonDates, Action<T> setDocumentDates)
    {
        _serializer = serializer;
        _applyJsonDates = applyJsonDates;
        _setDocumentDates = setDocumentDates;
    }

    /// <summary>Throws when <paramref name="operation"/> is a JSON patch that targets the version.</summary>
    public static void ValidateOperation(IPatchOperation operation)
    {
        if (operation is not JsonPatch { Patch.Operations: { } operations })
            return;

        foreach (var op in operations)
        {
            string path = op.Path?.TrimStart('/') ?? String.Empty;
            if (String.Equals(path, "version", StringComparison.OrdinalIgnoreCase))
                throw new ArgumentException("A JSON patch can't change the document version. The version is Elasticsearch metadata (_seq_no/_primary_term), not part of the document; use IfVersion to require a version.", nameof(operation));
        }
    }

    /// <summary>
    /// Returns a patched copy of <paramref name="hit"/>'s document, or <c>null</c> when an action patch made no change.
    /// </summary>
    public T? Apply(FindHit<T> hit, IPatchOperation operation)
    {
        var source = hit.Document ?? throw new DocumentException($"Document {hit.GetIndex()}/{hit.Id} returned no source");
        T? patched = operation switch
        {
            JsonPatch jsonPatch => ApplyJsonPatch(source, jsonPatch),
            ActionPatch<T> actionPatch => ApplyActionPatch(source, actionPatch),
            PartialPatch partialPatch => ApplyPartialPatch(source, partialPatch),
            _ => throw new ArgumentException("Unknown operation type", nameof(operation))
        };

        patched?.SetDocumentVersion(hit.GetElasticVersion());
        return patched;
    }

    private T ApplyJsonPatch(T source, JsonPatch operation)
    {
        JsonNode? target = JsonNode.Parse(_serializer.SerializeToString(source))
            ?? throw new DocumentException("Document JSON parsed to null; cannot apply JSON patch.");
        new JsonPatcher().Patch(ref target, operation.Patch);
        if (target is null)
            throw new DocumentException("JSON patch produced a null document.");

        _applyJsonDates(target);
        return Deserialize(target, "JSON patch produced a null document.");
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
        var sourceNode = JsonNode.Parse(_serializer.SerializeToString(source))
            ?? throw new DocumentException("Failed to parse source document JSON for partial merge.");
        if (sourceNode is JsonObject sourceObject && JsonNode.Parse(_serializer.SerializeToString(operation.Document)) is JsonObject partialObject)
        {
            foreach (var property in partialObject)
                sourceObject[property.Key] = property.Value?.DeepClone();
        }

        return Deserialize(sourceNode, "Partial merge produced a null document.");
    }

    private T Deserialize(JsonNode node, string nullMessage)
    {
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(node.ToJsonString()));
        return _serializer.Deserialize<T>(stream) ?? throw new DocumentException(nullMessage);
    }
}
