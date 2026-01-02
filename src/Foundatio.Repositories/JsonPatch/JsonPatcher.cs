using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using Foundatio.Repositories.Extensions;

namespace Foundatio.Repositories.Utility;

/// <summary>
/// Applies JSON Patch operations to a JSON document.
/// Converted from Newtonsoft.Json (JToken) to System.Text.Json (JsonNode) to align with
/// Elastic.Clients.Elasticsearch which exclusively uses System.Text.Json for serialization.
/// </summary>
public class JsonPatcher : AbstractPatcher<JsonNode>
{
    protected override JsonNode Replace(ReplaceOperation operation, JsonNode target)
    {
        var tokens = target.SelectPatchTokens(operation.Path).ToList();
        if (tokens.Count == 0)
        {
            string[] parts = operation.Path.Split('/');
            string parentPath = String.Join("/", parts.Select((p, i) => i < parts.Length - 1 ? p : String.Empty).Where(p => p.Length > 0));
            string propertyName = parts.LastOrDefault();

            if (target.SelectOrCreatePatchToken(parentPath) is not JsonObject parent)
                return target;

            parent[propertyName] = operation.Value?.DeepClone();

            return target;
        }

        foreach (var token in tokens)
        {
            var parent = token.Parent;
            if (parent is JsonObject parentObj)
            {
                var propName = parentObj.FirstOrDefault(p => ReferenceEquals(p.Value, token)).Key;
                if (propName != null)
                    parentObj[propName] = operation.Value?.DeepClone();
            }
            else if (parent is JsonArray parentArr)
            {
                var index = parentArr.ToList().IndexOf(token);
                if (index >= 0)
                    parentArr[index] = operation.Value?.DeepClone();
            }
            else // root object
            {
                return operation.Value?.DeepClone();
            }
        }

        return target;
    }

    protected override void Add(AddOperation operation, JsonNode target)
    {
        string[] parts = operation.Path.Split('/');
        string parentPath = String.Join("/", parts.Select((p, i) => i < parts.Length - 1 ? p : String.Empty).Where(p => p.Length > 0));
        string propertyName = parts.LastOrDefault();

        if (propertyName == "-")
        {
            var array = target.SelectOrCreatePatchArrayToken(parentPath) as JsonArray;
            array?.Add(operation.Value?.DeepClone());
        }
        else if (propertyName.IsNumeric())
        {
            var array = target.SelectOrCreatePatchArrayToken(parentPath) as JsonArray;
            if (Int32.TryParse(propertyName, out int index))
                array?.Insert(index, operation.Value?.DeepClone());
        }
        else
        {
            var parent = target.SelectOrCreatePatchToken(parentPath) as JsonObject;
            if (parent != null)
            {
                if (parent.ContainsKey(propertyName))
                    parent[propertyName] = operation.Value?.DeepClone();
                else
                    parent.Add(propertyName, operation.Value?.DeepClone());
            }
        }
    }

    protected override void Remove(RemoveOperation operation, JsonNode target)
    {
        string[] parts = operation.Path.Split('/');
        if (parts.Length == 0)
            return;

        string parentPath = String.Join("/", parts.Select((p, i) => i < parts.Length - 1 ? p : String.Empty).Where(p => p.Length > 0));
        string propertyName = parts.LastOrDefault();

        if (String.IsNullOrEmpty(propertyName))
            return;

        var parent = target.SelectPatchToken(parentPath);
        if (parent is JsonObject parentObj)
        {
            // Remove by property name (works even if value is null)
            if (parentObj.ContainsKey(propertyName))
                parentObj.Remove(propertyName);
        }
        else if (parent is JsonArray parentArr)
        {
            if (int.TryParse(propertyName, out int index) && index >= 0 && index < parentArr.Count)
                parentArr.RemoveAt(index);
        }
    }

    protected override void Move(MoveOperation operation, JsonNode target)
    {
        if (operation.Path.StartsWith(operation.FromPath))
            throw new ArgumentException("To path cannot be below from path");

        var token = target.SelectPatchToken(operation.FromPath);
        Remove(new RemoveOperation { Path = operation.FromPath }, target);
        Add(new AddOperation { Path = operation.Path, Value = token?.DeepClone() }, target);
    }

    protected override void Test(TestOperation operation, JsonNode target)
    {
        var existingValue = target.SelectPatchToken(operation.Path);
        if (!JsonNode.DeepEquals(existingValue, operation.Value))
        {
            throw new InvalidOperationException("Value at " + operation.Path + " does not match.");
        }
    }

    protected override void Copy(CopyOperation operation, JsonNode target)
    {
        var token = target.SelectPatchToken(operation.FromPath);
        Add(new AddOperation { Path = operation.Path, Value = token?.DeepClone() }, target);
    }
}

/// <summary>
/// Extension methods for JsonNode to support JSON Pointer paths used in JSON Patch.
/// </summary>
public static class JsonNodeExtensions
{
    public static JsonNode SelectPatchToken(this JsonNode token, string path)
    {
        return SelectToken(token, path.ToJsonPointerPath());
    }

    public static IEnumerable<JsonNode> SelectPatchTokens(this JsonNode token, string path)
    {
        var result = SelectToken(token, path.ToJsonPointerPath());
        if (result != null)
            yield return result;
    }

    private static JsonNode SelectToken(JsonNode node, string[] pathParts)
    {
        JsonNode current = node;
        foreach (var part in pathParts)
        {
            if (current == null)
                return null;

            if (current is JsonObject obj)
            {
                if (!obj.TryGetPropertyValue(part, out var value))
                    return null;
                current = value;
            }
            else if (current is JsonArray arr)
            {
                if (!int.TryParse(part, out int index) || index < 0 || index >= arr.Count)
                    return null;
                current = arr[index];
            }
            else
            {
                return null;
            }
        }

        return current;
    }

    public static JsonNode SelectOrCreatePatchToken(this JsonNode token, string path)
    {
        var pathParts = path.ToJsonPointerPath();
        if (pathParts.Length == 0)
            return token;

        // First pass: validate that the path can be created
        // Check that we won't encounter a numeric part where no array/object exists
        JsonNode current = token;
        for (int i = 0; i < pathParts.Length; i++)
        {
            string part = pathParts[i];

            if (current is JsonObject currentObj)
            {
                if (currentObj.TryGetPropertyValue(part, out var partToken))
                {
                    current = partToken;
                }
                else
                {
                    // Can't create numeric paths as objects - that would need to be an array
                    if (part.IsNumeric())
                        return null;
                    // Simulate continuing with the path (current becomes a placeholder for new object)
                    current = null;
                }
            }
            else if (current is JsonArray currentArr)
            {
                // Navigate through existing array elements
                if (int.TryParse(part, out int index) && index >= 0 && index < currentArr.Count)
                {
                    current = currentArr[index];
                }
                else
                {
                    return null;
                }
            }
            else if (current == null)
            {
                // We're past a part that needs to be created
                if (part.IsNumeric())
                    return null;
                // Continue validation
            }
            else
            {
                return null;
            }
        }

        // Second pass: actually create the missing parts
        current = token;
        for (int i = 0; i < pathParts.Length; i++)
        {
            string part = pathParts[i];

            if (current is JsonObject currentObj)
            {
                if (currentObj.TryGetPropertyValue(part, out var partToken))
                {
                    current = partToken;
                }
                else
                {
                    var newObj = new JsonObject();
                    currentObj[part] = newObj;
                    current = newObj;
                }
            }
            else if (current is JsonArray currentArr)
            {
                if (int.TryParse(part, out int index) && index >= 0 && index < currentArr.Count)
                {
                    current = currentArr[index];
                }
                else
                {
                    return null;
                }
            }
            else
            {
                return null;
            }
        }

        return current;
    }

    public static JsonNode SelectOrCreatePatchArrayToken(this JsonNode token, string path)
    {
        var pathParts = path.ToJsonPointerPath();
        if (pathParts.Length == 0)
            return token;

        // First pass: validate that the path can be created
        // Check that we won't encounter a numeric part where no array exists
        JsonNode current = token;
        var partsToCreate = new List<(JsonObject parent, string name, bool isLast)>();

        for (int i = 0; i < pathParts.Length; i++)
        {
            string part = pathParts[i];
            bool isLastPart = i == pathParts.Length - 1;

            if (current is JsonObject currentObj)
            {
                if (currentObj.TryGetPropertyValue(part, out var partToken))
                {
                    current = partToken;
                }
                else
                {
                    // Can't create numeric paths as objects - that would need to be an array
                    if (part.IsNumeric())
                        return null;
                    // Mark for creation, but don't create yet
                    partsToCreate.Add((currentObj, part, isLastPart));
                    // For validation, pretend we have a JsonObject here
                    current = null; // Will be created later
                }
            }
            else if (current is JsonArray currentArr)
            {
                // Navigate through existing array elements
                if (int.TryParse(part, out int index) && index >= 0 && index < currentArr.Count)
                {
                    current = currentArr[index];
                }
                else
                {
                    return null;
                }
            }
            else if (current == null)
            {
                // We're past a part that needs to be created
                if (part.IsNumeric())
                    return null;
                // Continue validation without tracking (we'll create the chain later)
            }
            else
            {
                return null;
            }
        }

        // Second pass: actually create the missing parts
        current = token;
        for (int i = 0; i < pathParts.Length; i++)
        {
            string part = pathParts[i];
            bool isLastPart = i == pathParts.Length - 1;

            if (current is JsonObject currentObj)
            {
                if (currentObj.TryGetPropertyValue(part, out var partToken))
                {
                    current = partToken;
                }
                else
                {
                    JsonNode newNode = isLastPart ? new JsonArray() : new JsonObject();
                    currentObj[part] = newNode;
                    current = newNode;
                }
            }
            else if (current is JsonArray currentArr)
            {
                if (int.TryParse(part, out int index) && index >= 0 && index < currentArr.Count)
                {
                    current = currentArr[index];
                }
                else
                {
                    return null;
                }
            }
            else
            {
                return null;
            }
        }

        return current;
    }

    /// <summary>
    /// Converts a JSON Patch path to an array of path segments.
    /// </summary>
    private static string[] ToJsonPointerPath(this string path)
    {
        if (String.IsNullOrEmpty(path))
            return Array.Empty<string>();

        // JSON Pointer format: /foo/bar/0
        return path.Split(new[] { '/' }, StringSplitOptions.RemoveEmptyEntries);
    }
}
