using System;
using System.Collections.Generic;
using System.Threading.Tasks;
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
/// Elasticsearch's <c>detect_noop</c> (enabled by default) automatically returns a no-op when
/// the partial document does not change any field values. However, automatic date tracking
/// injects <c>UpdatedUtc</c>, which typically prevents noop detection for <see cref="Models.IHaveDates"/> models.
/// <para><b>Cache invalidation:</b> Uses ID-based invalidation only. The Update API executes
/// server-side and does not return the modified document to the client.</para>
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
/// <remarks>
/// Actions that return <c>bool</c> signal whether the document was modified. When all actions
/// return <c>false</c>, the write to Elasticsearch is skipped entirely (no Index call, no date
/// tracking, no cache invalidation). Actions that use <see cref="Action{T}"/> are assumed to
/// always modify the document.
/// <para><b>Cache invalidation:</b> Uses document-based invalidation. The modified <typeparamref name="T"/>
/// is passed to <c>InvalidateCacheAsync</c>, so custom cache key overrides work correctly.</para>
/// </remarks>
public class ActionPatch<T> : IPatchOperation where T : class
{
    /// <summary>
    /// Initializes a new instance of the <see cref="ActionPatch{T}"/> class with a single action
    /// that always signals modification.
    /// </summary>
    /// <param name="changeAction">The action to apply to the document.</param>
    public ActionPatch(Action<T> changeAction)
    {
        ArgumentNullException.ThrowIfNull(changeAction);

        Actions.Add(doc =>
        {
            changeAction(doc);
            return true;
        });
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ActionPatch{T}"/> class with a single action
    /// that returns whether the document was modified.
    /// </summary>
    /// <param name="changeFunc">A function that modifies the document and returns <c>true</c> if modified, <c>false</c> for no-op.</param>
    public ActionPatch(Func<T, bool> changeFunc)
    {
        ArgumentNullException.ThrowIfNull(changeFunc);

        Actions.Add(changeFunc);
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ActionPatch{T}"/> class with multiple actions
    /// that always signal modification.
    /// </summary>
    /// <param name="changeActions">The actions to apply to the document.</param>
    public ActionPatch(params Action<T>[] changeActions)
    {
        ArgumentNullException.ThrowIfNull(changeActions);

        foreach (var action in changeActions)
        {
            ArgumentNullException.ThrowIfNull(action);
            Actions.Add(doc =>
            {
                action(doc);
                return true;
            });
        }
    }

    /// <summary>
    /// Gets the collection of actions to apply to the document. Each returns <c>true</c> if the document was modified.
    /// </summary>
    public ICollection<Func<T, bool>> Actions { get; } = new List<Func<T, bool>>();
}

/// <summary>
/// A patch operation that applies a JSON Patch document (RFC 6902) to a document.
/// </summary>
/// <remarks>
/// Uses a get-modify-reindex pattern, so a write always occurs and <c>PatchAsync</c> always
/// returns <c>true</c>. Empty patch documents (no operations) return <c>false</c>.
/// <para><b>Cache invalidation:</b> Bulk operations (<c>PatchAllAsync</c>) use document-based
/// invalidation. Single-document <c>PatchAsync</c> uses ID-based invalidation because it
/// operates on raw JSON rather than a typed document.</para>
/// </remarks>
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
/// <remarks>
/// Elasticsearch does not automatically detect noops for script updates.
/// To signal a no-op, the script must explicitly set <c>ctx.op = 'none'</c>.
/// Painless uses <c>==</c> for equality (Java-style); <c>===</c> is not valid.
/// <para><b>Cache invalidation:</b> Uses ID-based invalidation only. Scripts execute
/// server-side and do not return the modified document to the client.</para>
/// </remarks>
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
    public Dictionary<string, object>? Params { get; set; }
}

/// <summary>
/// Extension methods for applying <see cref="ActionPatch{T}"/> operations.
/// </summary>
public static class ActionPatchExtensions
{
    /// <inheritdoc cref="IRepository{T}.PatchAsync(Id, IPatchOperation, ICommandOptions)"/>
    public static Task<bool> PatchAsync<T>(this IRepository<T> repository, Id id, ActionPatch<T> operation, ICommandOptions? options = null) where T : class, IIdentity, new()
    {
        return repository.PatchAsync(id, operation, options);
    }

    /// <inheritdoc cref="IRepository{T}.PatchAsync(Id, IPatchOperation, CommandOptionsDescriptor{T})"/>
    public static Task<bool> PatchAsync<T>(this IRepository<T> repository, Id id, ActionPatch<T> operation, CommandOptionsDescriptor<T> options) where T : class, IIdentity, new()
    {
        return repository.PatchAsync(id, operation, options);
    }
}
