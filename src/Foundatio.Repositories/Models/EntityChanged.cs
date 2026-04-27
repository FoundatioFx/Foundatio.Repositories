using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using Foundatio.Utility;

namespace Foundatio.Repositories.Models;

[DebuggerDisplay("{Type} {ChangeType}: Id={Id}")]
public class EntityChanged : IHaveData
{
    public EntityChanged()
    {
    }

    /// <summary>
    /// The entity type name. May be null for non-entity-specific change notifications.
    /// </summary>
    public string? Type { get; set; }

    /// <summary>
    /// The entity identifier. Null for bulk or collection-level change notifications
    /// that do not target a single document.
    /// </summary>
    public string? Id { get; set; }
    public ChangeType ChangeType { get; set; }
    [DisallowNull]
    public IDictionary<string, object?> Data { get => field; set => field = value ?? new DataDictionary(); } = new DataDictionary();
}
