using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
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
        // Handle JSONPath expressions (e.g., $.books[?(@.author == 'X')])
        if (operation.Path.StartsWith("$.", StringComparison.Ordinal) || operation.Path.StartsWith("$[", StringComparison.Ordinal))
        {
            var tokens = target.SelectPatchTokens(operation.Path).ToList();
            foreach (var token in tokens)
            {
                var tokenParent = token.Parent;
                if (tokenParent is JsonArray arr)
                {
                    for (int i = 0; i < arr.Count; i++)
                    {
                        if (ReferenceEquals(arr[i], token))
                        {
                            arr.RemoveAt(i);
                            break;
                        }
                    }
                }
                else if (tokenParent is JsonObject tokenParentObj)
                {
                    var key = tokenParentObj.FirstOrDefault(p => ReferenceEquals(p.Value, token)).Key;
                    if (key != null)
                        tokenParentObj.Remove(key);
                }
            }
            return;
        }

        string[] parts = operation.Path.Split('/');
        if (parts.Length == 0)
            return;

        string parentPath = String.Join("/", parts.Select((p, i) => i < parts.Length - 1 ? p : String.Empty).Where(p => p.Length > 0));
        string propertyName = parts.LastOrDefault();

        if (String.IsNullOrEmpty(propertyName))
            return;

        var parent = target.SelectPatchToken(parentPath);
        if (parent is JsonObject parentObjPointer)
        {
            if (parentObjPointer.ContainsKey(propertyName))
                parentObjPointer.Remove(propertyName);
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
            throw new InvalidOperationException($"Value at {operation.Path} does not match.");
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
        if (path.StartsWith("$.", StringComparison.Ordinal) || path.StartsWith("$[", StringComparison.Ordinal))
            return SelectJsonPathTokens(token, path);

        var result = SelectToken(token, path.ToJsonPointerPath());
        if (result != null)
            return new[] { result };
        return Enumerable.Empty<JsonNode>();
    }

    /// <summary>
    /// Evaluates a subset of JSONPath expressions against a JSON node.
    /// Supports filter expressions like <c>$.array[?(@.prop == 'value')]</c> and <c>$.array[?(@ == 'value')]</c>.
    /// </summary>
    private static IEnumerable<JsonNode> SelectJsonPathTokens(JsonNode root, string path)
    {
        // Strip leading $
        string remaining = path.StartsWith("$.", StringComparison.Ordinal) ? path[2..] : path[1..];

        // Split on dots, but respect brackets
        var segments = SplitJsonPathSegments(remaining);
        IEnumerable<JsonNode> current = new[] { root };

        foreach (var segment in segments)
        {
            var next = new List<JsonNode>();
            foreach (var node in current)
            {
                if (node is null) continue;

                // Segment like "books[?(@.author == 'X')]" or "books[?(@.author == 'X')].prop"
                // or pure filter "[?(@.author == 'X')]"
                int bracketStart = segment.IndexOf('[');
                if (bracketStart >= 0)
                {
                    // Navigate to named property first (if any)
                    JsonNode target2 = node;
                    if (bracketStart > 0)
                    {
                        string propName = segment[..bracketStart];
                        if (node is JsonObject propObj && propObj.TryGetPropertyValue(propName, out var propVal) && propVal is not null)
                            target2 = propVal;
                        else
                            continue;
                    }

                    // Extract the bracket expression
                    int bracketEnd = segment.LastIndexOf(']');
                    if (bracketEnd < 0) continue;
                    string expr = segment[(bracketStart + 1)..bracketEnd];

                    // Filter expression: [?(...)]
                    if (expr.StartsWith("?(", StringComparison.Ordinal) && expr.EndsWith(')'))
                    {
                        string filter = expr[2..^1];
                        if (target2 is JsonArray arr)
                            next.AddRange(arr.Where(item => item is not null && EvaluateJsonPathFilter(item, filter)).Select(item => item!));
                    }
                }
                else
                {
                    if (node is JsonObject obj && obj.TryGetPropertyValue(segment, out var value) && value is not null)
                        next.Add(value);
                }
            }
            current = next;
        }

        return current;
    }

    private static bool EvaluateJsonPathFilter(JsonNode node, string filter)
    {
        // Pattern: @.property == 'value' or @.property == value
        var dotPropMatch = Regex.Match(filter,
            @"^@\.(\w+)\s*==\s*'(.+)'$");
        if (dotPropMatch.Success)
        {
            string prop = dotPropMatch.Groups[1].Value;
            string expected = dotPropMatch.Groups[2].Value;
            if (node is JsonObject obj && obj.TryGetPropertyValue(prop, out var val))
                return val?.GetValue<string>() == expected;
            return false;
        }

        // Pattern: @ == 'value' (match array element value directly)
        var directMatch = Regex.Match(filter,
            @"^@\s*==\s*'(.+)'$");
        if (directMatch.Success)
        {
            string expected = directMatch.Groups[1].Value;
            if (node is JsonValue jsonVal)
            {
                try { return jsonVal.GetValue<string>() == expected; }
                catch { return false; }
            }
            return false;
        }

        return false;
    }

    private static List<string> SplitJsonPathSegments(string path)
    {
        var segments = new List<string>();
        int start = 0;
        int depth = 0;

        for (int i = 0; i < path.Length; i++)
        {
            char c = path[i];
            if (c == '[') depth++;
            else if (c == ']') depth--;
            else if (c == '.' && depth == 0)
            {
                if (i > start)
                    segments.Add(path[start..i]);
                start = i + 1;
            }
        }

        if (start < path.Length)
            segments.Add(path[start..]);

        return segments;
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
                    current = null; // Will be created in the second pass
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

        if (path.StartsWith('$'))
            throw new NotSupportedException($"JSONPath expressions are not supported in patch operations. Use JSON Pointer format (e.g., '/foo/bar') instead of JSONPath (e.g., '$.foo.bar'). Path: {path}");

        return path.Split(new[] { '/' }, StringSplitOptions.RemoveEmptyEntries);
    }
}
