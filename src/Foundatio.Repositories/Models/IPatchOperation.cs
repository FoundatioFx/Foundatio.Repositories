using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Utility;

namespace Foundatio.Repositories.Models;

public interface IPatchOperation { }

public class PartialPatch : IPatchOperation
{
    public PartialPatch(object document)
    {
        Document = document ?? throw new ArgumentNullException(nameof(document));
    }

    public object Document { get; }
}

public class ActionPatch<T> : IPatchOperation where T : class
{
    public ActionPatch(Action<T> changeAction)
    {
        if (changeAction == null)
            throw new ArgumentNullException(nameof(changeAction));

        Actions.Add(changeAction);
    }

    public ActionPatch(params Action<T>[] changeActions)
    {
        Actions.AddRange(changeActions);
    }

    public ICollection<Action<T>> Actions { get; } = new List<Action<T>>();
}

public class JsonPatch : IPatchOperation
{
    public JsonPatch(PatchDocument patch)
    {
        Patch = patch ?? throw new ArgumentNullException(nameof(patch));
    }

    public PatchDocument Patch { get; }
}

public class ScriptPatch : IPatchOperation
{
    public ScriptPatch(string script)
    {
        if (String.IsNullOrEmpty(script))
            throw new ArgumentNullException(nameof(script));

        Script = script;
    }

    public string Script { get; }
    public Dictionary<string, object> Params { get; set; }
}

public static class ActionPatchExtensions
{
    public static Task PatchAsync<T>(this IRepository<T> repository, Id id, ActionPatch<T> operation, ICommandOptions options = null) where T : class, IIdentity, new()
    {
        return repository.PatchAsync(id, operation, options);
    }

    public static Task PatchAsync<T>(this IRepository<T> repository, Id id, ActionPatch<T> operation, CommandOptionsDescriptor<T> options) where T : class, IIdentity, new()
    {
        return repository.PatchAsync(id, operation, options);
    }
}
