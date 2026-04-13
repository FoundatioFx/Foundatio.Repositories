using System;
using System.Collections.Generic;
using System.Linq;
using Foundatio.Repositories.Extensions;
using Newtonsoft.Json.Linq;

namespace Foundatio.Repositories.Utility;

public class JsonPatcher : AbstractPatcher<JToken>
{
    protected override JToken Replace(ReplaceOperation operation, JToken target)
    {
        if (operation.Path is null)
            return operation.Value ?? JValue.CreateNull();

        var tokens = target.SelectPatchTokens(operation.Path).ToList();
        if (tokens.Count == 0)
        {
            string[] parts = operation.Path.Split('/');
            string parentPath = String.Join("/", parts.Select((p, i) => i < parts.Length - 1 ? p : String.Empty).Where(p => p.Length > 0));
            string? propertyName = parts.LastOrDefault();

            if (propertyName is null || target.SelectOrCreatePatchToken(parentPath) is not JObject parent)
                return target;

            parent[propertyName] = operation.Value;

            return target;
        }

        foreach (var token in tokens)
        {
            if (token.Parent != null)
                token.Replace(operation.Value ?? JValue.CreateNull());
            else // root object
                return operation.Value ?? JValue.CreateNull();
        }

        return target;
    }

    protected override void Add(AddOperation operation, JToken target)
    {
        if (operation.Path is null)
            return;

        string[] parts = operation.Path.Split('/');
        string parentPath = String.Join("/", parts.Select((p, i) => i < parts.Length - 1 ? p : String.Empty).Where(p => p.Length > 0));
        string? propertyName = parts.LastOrDefault();

        var value = operation.Value ?? JValue.CreateNull();

        if (propertyName == "-")
        {
            var array = target.SelectOrCreatePatchArrayToken(parentPath) as JArray;
            array?.Add(value);
        }
        else if (propertyName is not null && propertyName.IsNumeric())
        {
            var array = target.SelectOrCreatePatchArrayToken(parentPath) as JArray;
            if (Int32.TryParse(propertyName, out int index))
                array?.Insert(index, value);
        }
        else if (propertyName is not null)
        {
            var parent = target.SelectOrCreatePatchToken(parentPath) as JObject;
            var property = parent?.Property(propertyName);
            if (property == null)
                parent?.Add(propertyName, value);
            else
                property.Value = value;
        }
    }

    protected override void Remove(RemoveOperation operation, JToken target)
    {
        if (operation.Path is null)
            return;

        var tokens = target.SelectPatchTokens(operation.Path).ToList();
        if (tokens.Count == 0)
            return;

        foreach (var token in tokens)
        {
            if (token.Parent is JProperty)
            {
                token.Parent.Remove();
            }
            else
            {
                token.Remove();
            }
        }
    }

    protected override void Move(MoveOperation operation, JToken target)
    {
        if (operation.Path is null || operation.FromPath is null)
            throw new ArgumentException("Move operation requires both 'path' and 'from' properties.");

        if (operation.Path.StartsWith(operation.FromPath))
            throw new ArgumentException("To path cannot be below from path");

        var token = target.SelectPatchToken(operation.FromPath);
        if (token is null)
            throw new InvalidOperationException($"Move source path '{operation.FromPath}' does not exist.");

        Remove(new RemoveOperation { Path = operation.FromPath }, target);
        Add(new AddOperation { Path = operation.Path, Value = token }, target);
    }

    protected override void Test(TestOperation operation, JToken target)
    {
        // Do I need to clone this?
        var existingValue = operation.Path is not null ? target.SelectPatchToken(operation.Path) : null;
        if (existingValue is null || !JToken.DeepEquals(existingValue, operation.Value))
        {
            throw new InvalidOperationException("Value at " + operation.Path + " does not match.");
        }
    }

    protected override void Copy(CopyOperation operation, JToken target)
    {
        if (operation.FromPath is null)
            throw new ArgumentException("Copy operation requires a 'from' property.");

        var token = target.SelectPatchToken(operation.FromPath);  // Do I need to clone this?
        if (token is null)
            throw new InvalidOperationException($"Copy source path '{operation.FromPath}' does not exist.");

        Add(new AddOperation { Path = operation.Path, Value = token }, target);
    }
}

public static class JTokenExtensions
{
    public static JToken? SelectPatchToken(this JToken token, string path)
    {
        return token.SelectToken(path.ToJTokenPath());
    }

    public static IEnumerable<JToken> SelectPatchTokens(this JToken token, string path)
    {
        return token.SelectTokens(path.ToJTokenPath());
    }

    public static JToken? SelectOrCreatePatchToken(this JToken token, string path)
    {
        var result = token.SelectToken(path.ToJTokenPath());
        if (result != null)
            return result;

        string[] parts = path.Split(new[] { '/' }, StringSplitOptions.RemoveEmptyEntries);
        if (parts.Any(p => p.IsNumeric()))
            return null;

        JToken current = token;
        for (int i = 0; i < parts.Length; i++)
        {
            string part = parts[i];
            var partToken = current.SelectPatchToken(part);
            if (partToken == null)
            {
                if (current is JObject partObject)
                    current = partObject[part] = new JObject();
            }
            else
            {
                current = partToken;
            }
        }

        return current;
    }

    public static JToken? SelectOrCreatePatchArrayToken(this JToken token, string path)
    {
        var result = token.SelectToken(path.ToJTokenPath());
        if (result != null)
            return result;

        string[] parts = path.Split(new[] { '/' }, StringSplitOptions.RemoveEmptyEntries);
        if (parts.Any(p => p.IsNumeric()))
            return null;

        JToken current = token;
        for (int i = 0; i < parts.Length; i++)
        {
            string part = parts[i];
            var partToken = current.SelectPatchToken(part);
            if (partToken == null)
            {
                if (current is JObject partObject)
                {
                    bool isLastPart = i == parts.Length - 1;
                    current = partObject[part] = isLastPart ? new JArray() : new JObject();
                }
            }
            else
            {
                current = partToken;
            }
        }

        return current;
    }
}
