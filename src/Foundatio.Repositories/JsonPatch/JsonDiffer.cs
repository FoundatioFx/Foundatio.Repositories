using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Foundatio.Repositories.Utility;

/// <summary>
/// Computes the difference between two JSON documents and generates a JSON Patch document.
/// Converted from Newtonsoft.Json (JToken) to System.Text.Json (JsonNode) to align with
/// Elastic.Clients.Elasticsearch which exclusively uses System.Text.Json for serialization.
/// </summary>
public class JsonDiffer
{
    internal static string Extend(string path, string extension)
    {
        // TODO: JSON property name needs escaping for path ??
        return path + "/" + extension;
    }

    private static Operation Build(string op, string path, string key, JsonNode value)
    {
        if (String.IsNullOrEmpty(key))
            return Operation.Parse("{ \"op\" : \"" + op + "\" , \"path\": \"" + path + "\", \"value\": " +
                                (value == null ? "null" : value.ToJsonString()) + "}");

        return Operation.Parse("{ \"op\" : \"" + op + "\" , \"path\" : \"" + Extend(path, key) + "\" , \"value\" : " +
                            (value == null ? "null" : value.ToJsonString()) + "}");
    }

    internal static Operation Add(string path, string key, JsonNode value)
    {
        return Build("add", path, key, value);
    }

    internal static Operation Remove(string path, string key)
    {
        return Build("remove", path, key, null);
    }

    internal static Operation Replace(string path, string key, JsonNode value)
    {
        return Build("replace", path, key, value);
    }

    internal static IEnumerable<Operation> CalculatePatch(JsonNode left, JsonNode right, bool useIdToDetermineEquality,
        string path = "")
    {
        if (GetNodeType(left) != GetNodeType(right))
        {
            yield return JsonDiffer.Replace(path, "", right);
            yield break;
        }

        if (left is JsonArray)
        {
            Operation prev = null;
            foreach (var operation in ProcessArray(left, right, path, useIdToDetermineEquality))
            {
                if (prev is RemoveOperation prevRemove && operation is AddOperation add && add.Path == prevRemove.Path)
                {
                    yield return Replace(add.Path, "", add.Value);
                    prev = null;
                }
                else
                {
                    if (prev != null)
                        yield return prev;
                    prev = operation;
                }
            }

            if (prev != null)
                yield return prev;
        }
        else if (left is JsonObject leftObj && right is JsonObject rightObj)
        {
            var lprops = leftObj.OrderBy(p => p.Key).ToList();
            var rprops = rightObj.OrderBy(p => p.Key).ToList();

            foreach (var removed in lprops.Where(l => !rprops.Any(r => r.Key == l.Key)))
            {
                yield return JsonDiffer.Remove(path, removed.Key);
            }

            foreach (var added in rprops.Where(r => !lprops.Any(l => l.Key == r.Key)))
            {
                yield return JsonDiffer.Add(path, added.Key, added.Value);
            }

            var matchedKeys = lprops.Select(x => x.Key).Intersect(rprops.Select(y => y.Key));
            var zipped = matchedKeys.Select(k => new { key = k, left = leftObj[k], right = rightObj[k] });

            foreach (var match in zipped)
            {
                string newPath = path + "/" + match.key;
                foreach (var patch in CalculatePatch(match.left, match.right, useIdToDetermineEquality, newPath))
                    yield return patch;
            }
            yield break;
        }
        else
        {
            // Two values, same type, not JsonObject so no properties

            if (JsonNodeToString(left) == JsonNodeToString(right))
                yield break;
            else
                yield return JsonDiffer.Replace(path, "", right);
        }
    }

    private static string GetNodeType(JsonNode node)
    {
        return node switch
        {
            null => "null",
            JsonObject => "object",
            JsonArray => "array",
            JsonValue v => v.GetValueKind().ToString(),
            _ => "unknown"
        };
    }

    private static string JsonNodeToString(JsonNode node)
    {
        return node?.ToJsonString() ?? "null";
    }

    private static IEnumerable<Operation> ProcessArray(JsonNode left, JsonNode right, string path,
        bool useIdPropertyToDetermineEquality)
    {
        var comparer = new CustomCheckEqualityComparer(useIdPropertyToDetermineEquality);

        int commonHead = 0;
        int commonTail = 0;
        var array1 = (left as JsonArray)?.ToArray() ?? Array.Empty<JsonNode>();
        int len1 = array1.Length;
        var array2 = (right as JsonArray)?.ToArray() ?? Array.Empty<JsonNode>();
        int len2 = array2.Length;
        //    if (len1 == 0 && len2 ==0 ) yield break;
        while (commonHead < len1 && commonHead < len2)
        {
            if (comparer.Equals(array1[commonHead], array2[commonHead]) == false)
                break;

            //diff and yield objects here
            foreach (var operation in CalculatePatch(array1[commonHead], array2[commonHead], useIdPropertyToDetermineEquality, path + "/" + commonHead))
            {
                yield return operation;
            }
            commonHead++;
        }

        // separate common tail
        while (commonTail + commonHead < len1 && commonTail + commonHead < len2)
        {
            if (comparer.Equals(array1[len1 - 1 - commonTail], array2[len2 - 1 - commonTail]) == false)
                break;

            int index1 = len1 - 1 - commonTail;
            int index2 = len2 - 1 - commonTail;
            foreach (var operation in CalculatePatch(array1[index1], array2[index2], useIdPropertyToDetermineEquality, path + "/" + index1))
            {
                yield return operation;
            }
            commonTail++;
        }

        if (commonHead == 0 && commonTail == 0 && len2 > 0 && len1 > 0)
        {
            yield return new ReplaceOperation
            {
                Path = path,
                Value = new JsonArray(array2.Select(n => n?.DeepClone()).ToArray())
            };
            yield break;
        }

        var leftMiddle = array1.Skip(commonHead).Take(array1.Length - commonTail - commonHead).ToArray();
        var rightMiddle = array2.Skip(commonHead).Take(array2.Length - commonTail - commonHead).ToArray();
        foreach (var jToken in leftMiddle)
        {
            yield return new RemoveOperation
            {
                Path = path + "/" + commonHead
            };
        }
        for (int i = 0; i < rightMiddle.Length; i++)
        {
            yield return new AddOperation
            {
                Value = rightMiddle[i]?.DeepClone(),
                Path = path + "/" + (i + commonHead)
            };
        }
    }

    public PatchDocument Diff(JsonNode @from, JsonNode to, bool useIdPropertyToDetermineEquality)
    {
        return new PatchDocument(CalculatePatch(@from, to, useIdPropertyToDetermineEquality).ToArray());
    }
}

internal class CustomCheckEqualityComparer : IEqualityComparer<JsonNode>
{
    private readonly bool _enableIdCheck;

    public CustomCheckEqualityComparer(bool enableIdCheck)
    {
        _enableIdCheck = enableIdCheck;
    }

    public bool Equals(JsonNode x, JsonNode y)
    {
        if (!_enableIdCheck || x is not JsonObject xObj || y is not JsonObject yObj)
            return JsonNode.DeepEquals(x, y);

        string xId = xObj["id"]?.GetValue<string>();
        string yId = yObj["id"]?.GetValue<string>();
        if (xId != null && xId == yId)
        {
            return true;
        }

        return JsonNode.DeepEquals(x, y);
    }

    public int GetHashCode(JsonNode obj)
    {
        if (!_enableIdCheck || obj is not JsonObject xObj)
            return obj?.ToJsonString()?.GetHashCode() ?? 0;

        string xId = xObj["id"]?.GetValue<string>();
        if (xId != null)
            return xId.GetHashCode() + (obj.ToJsonString()?.GetHashCode() ?? 0);

        return obj.ToJsonString()?.GetHashCode() ?? 0;
    }

    public static bool HaveEqualIds(JsonNode x, JsonNode y)
    {
        if (x is not JsonObject xObj || y is not JsonObject yObj)
            return false;

        string xId = xObj["id"]?.GetValue<string>();
        string yId = yObj["id"]?.GetValue<string>();

        return xId != null && xId == yId;
    }
}
