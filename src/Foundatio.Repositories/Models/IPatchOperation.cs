using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Utility;

namespace Foundatio.Repositories.Models;

/// <summary>
/// Marker interface for patch operations that can be applied to documents.
/// </summary>
/// <seealso cref="PartialPatch"/>
/// <seealso cref="ActionPatch{T}"/>
/// <seealso cref="JsonPatch"/>
/// <seealso cref="ScriptPatch"/>
public interface IPatchOperation { }

/// <summary>
/// A patch operation that updates a document using a partial document object.
/// </summary>
/// <remarks>
/// Only the non-null properties of the partial document will be applied to the target document.
/// </remarks>
public class PartialPatch : IPatchOperation
{
    /// <summary>
    /// Initializes a new instance of the <see cref="PartialPatch"/> class.
    /// </summary>
    /// <param name="document">An object containing the properties to update.</param>
    public PartialPatch(object document)
    {
        Document = document ?? throw new ArgumentNullException(nameof(document));
    }

    /// <summary>
    /// Gets the partial document containing properties to update.
    /// </summary>
    public object Document { get; }
}

/// <summary>
/// A patch operation that applies one or more actions to modify a document.
/// </summary>
/// <typeparam name="T">The document type.</typeparam>
public class ActionPatch<T> : IPatchOperation where T : class
{
    /// <summary>
    /// Initializes a new instance of the <see cref="ActionPatch{T}"/> class with a single action.
    /// </summary>
    /// <param name="changeAction">The action to apply to the document.</param>
    public ActionPatch(Action<T> changeAction)
    {
        if (changeAction == null)
            throw new ArgumentNullException(nameof(changeAction));

        Actions.Add(changeAction);
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ActionPatch{T}"/> class with multiple actions.
    /// </summary>
    /// <param name="changeActions">The actions to apply to the document.</param>
    public ActionPatch(params Action<T>[] changeActions)
    {
        Actions.AddRange(changeActions);
    }

    /// <summary>
    /// Gets the collection of actions to apply to the document.
    /// </summary>
    public ICollection<Action<T>> Actions { get; } = new List<Action<T>>();
}

/// <summary>
/// A patch operation that applies a JSON Patch document (RFC 6902) to a document.
/// </summary>
public class JsonPatch : IPatchOperation
{
    /// <summary>
    /// Initializes a new instance of the <see cref="JsonPatch"/> class.
    /// </summary>
    /// <param name="patch">The JSON Patch document to apply.</param>
    public JsonPatch(PatchDocument patch)
    {
        Patch = patch ?? throw new ArgumentNullException(nameof(patch));
    }

    /// <summary>
    /// Gets the JSON Patch document to apply.
    /// </summary>
    public PatchDocument Patch { get; }
}

/// <summary>
/// A patch operation that executes a script to modify a document (Elasticsearch-specific).
/// </summary>
public class ScriptPatch : IPatchOperation
{
    /// <summary>
    /// Initializes a new instance of the <see cref="ScriptPatch"/> class.
    /// </summary>
    /// <param name="script">The script to execute.</param>
    public ScriptPatch(string script)
    {
        if (String.IsNullOrEmpty(script))
            throw new ArgumentNullException(nameof(script));

        Script = script;
    }

    /// <summary>
    /// Gets the script to execute.
    /// </summary>
    public string Script { get; }

    /// <summary>
    /// Gets or sets the parameters to pass to the script.
    /// </summary>
    public Dictionary<string, object> Params { get; set; }
}

/// <summary>
/// Extension methods for applying <see cref="ActionPatch{T}"/> operations.
/// </summary>
public static class ActionPatchExtensions
{
    /// <inheritdoc cref="IRepository{T}.PatchAsync(Id, IPatchOperation, ICommandOptions)"/>
    public static Task PatchAsync<T>(this IRepository<T> repository, Id id, ActionPatch<T> operation, ICommandOptions options = null) where T : class, IIdentity, new()
    {
        return repository.PatchAsync(id, operation, options);
    }

    /// <inheritdoc cref="IRepository{T}.PatchAsync(Id, IPatchOperation, CommandOptionsDescriptor{T})"/>
    public static Task PatchAsync<T>(this IRepository<T> repository, Id id, ActionPatch<T> operation, CommandOptionsDescriptor<T> options) where T : class, IIdentity, new()
    {
        return repository.PatchAsync(id, operation, options);
    }
}
