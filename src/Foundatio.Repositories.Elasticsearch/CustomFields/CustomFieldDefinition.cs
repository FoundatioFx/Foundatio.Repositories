using System;
using System.Collections.Generic;
using Foundatio.Repositories.Models;
using Foundatio.Utility;

namespace Foundatio.Repositories.Elasticsearch.CustomFields;

public record CustomFieldDefinition : IIdentity, IHaveDates, ISupportSoftDeletes, IHaveData
{
    public string Id { get; set; }

    /// <summary>
    /// The entity type that this custom field is for.
    /// </summary>
    public string EntityType { get; set; }

    /// <summary>
    /// The tenant key which could be a composite key that this custom field belongs to. Each tenant can have it's own
    /// set of custom fields for an entity.
    /// </summary>
    public string TenantKey { get; set; }

    /// <summary>
    /// The friendly custom field name.
    /// </summary>
    public string Name { get; set; }

    /// <summary>
    /// The custom field description.
    /// </summary>
    public string Description { get; set; }

    /// <summary>
    /// Gets or sets the display order.
    /// </summary>
    public int DisplayOrder { get; set; }

    /// <summary>
    /// Sets the process mode for this custom field instance. Default is to only process the field when there is a value. <see cref="CustomFieldProcessMode.ProcessOnValue"/>
    /// Can be set to <see cref="CustomFieldProcessMode.AlwaysProcess"/> in order to always run the <see cref="ICustomFieldType.ProcessValueAsync{T}(T, object, CustomFieldDefinition)"/>
    /// even when a value is not present.
    /// </summary>
    public CustomFieldProcessMode ProcessMode { get; set; } = CustomFieldProcessMode.ProcessOnValue;

    /// <summary>
    /// Sets the order in which the custom field is processed. Custom fields in the <see cref="CustomFieldProcessMode.AlwaysProcess"/> mode will always
    /// run after fields in <see cref="CustomFieldProcessMode.ProcessOnValue"/> mode.
    /// </summary>
    public int ProcessOrder { get; set; }

    /// <summary>
    /// The type of index this custom field value should be stored in. (ie. string, number, float, address)
    /// </summary>
    public string IndexType { get; set; }

    /// <summary>
    /// The reserved indexing slot for this custom field. This name will be pooled across tenants for the same index
    /// type to keep dynamic indexes in Elasticsearch from exploding.
    /// </summary>
    public int IndexSlot { get; set; }

    /// <summary>
    /// Any additional custom data that needs to be associated to this custom field.
    /// </summary>
    public IDictionary<string, object> Data { get; set; } = new Dictionary<string, object>();

    /// <summary>
    /// Date the custom field was last updated.
    /// </summary>
    public DateTime UpdatedUtc { get; set; }

    /// <summary>
    /// Date the custom field was originally created.
    /// </summary>
    public DateTime CreatedUtc { get; set; }

    /// <summary>
    /// Whether this custom field has been soft deleted
    /// </summary>
    public bool IsDeleted { get; set; }

    private string _idxName = null;
    /// <summary>
    /// Returns the Elasticsearch sub-field name used under the <c>idx</c> object for this definition.
    /// The format is <c>{IndexType}-{IndexSlot}</c> (e.g., <c>"string-1"</c>, <c>"int-3"</c>).
    /// </summary>
    public string GetIdxName()
    {
        if (_idxName == null)
            _idxName = $"{IndexType}-{IndexSlot}";

        return _idxName;
    }
}

/// <summary>
/// Controls when a custom field's <see cref="ICustomFieldType.ProcessValueAsync{T}(T, object, CustomFieldDefinition)"/> is invoked during document save.
/// </summary>
public enum CustomFieldProcessMode
{
    /// <summary>
    /// Process only when the document's <c>Data</c> dictionary contains a value for this field. This is the default.
    /// </summary>
    ProcessOnValue,

    /// <summary>
    /// Always run the field type processor, even when no value is present. Use for calculated/computed fields
    /// that derive their value from other fields. These fields are always processed after all <see cref="ProcessOnValue"/> fields.
    /// </summary>
    AlwaysProcess
}

/// <summary>
/// Implement this interface on entities that need custom fields with full control over how
/// field values are read and written. Use instead of <see cref="IHaveCustomFields"/> when
/// custom field values are not stored in a flat <see cref="IHaveData.Data"/> dictionary.
/// </summary>
public interface IHaveVirtualCustomFields
{
    /// <summary>
    /// Returns all custom field values for this entity.
    /// </summary>
    IDictionary<string, object> GetCustomFields();

    /// <summary>
    /// Returns the value of a custom field by name, or <c>null</c> if not set.
    /// </summary>
    object GetCustomField(string name);

    /// <summary>
    /// Sets the value of a custom field by name.
    /// </summary>
    void SetCustomField(string name, object value);

    /// <summary>
    /// Removes a custom field by name.
    /// </summary>
    void RemoveCustomField(string name);

    /// <summary>
    /// Gets the indexed custom field values. The framework populates this automatically during save.
    /// </summary>
    IDictionary<string, object> Idx { get; }

    /// <summary>
    /// Returns the tenant key used to scope custom field definitions for this entity.
    /// </summary>
    string GetTenantKey();
}

/// <summary>
/// Implement this interface on entities that store custom field values in their <see cref="IHaveData.Data"/> dictionary.
/// The framework reads values from <c>Data</c>, processes them through <see cref="ICustomFieldType"/>, and writes
/// indexed values to <see cref="Idx"/> during save.
/// </summary>
public interface IHaveCustomFields : IHaveData
{
    /// <summary>
    /// Gets the indexed custom field values. The framework populates this automatically during save.
    /// </summary>
    IDictionary<string, object> Idx { get; }

    /// <summary>
    /// Returns the tenant key used to scope custom field definitions for this entity.
    /// </summary>
    string GetTenantKey();
}
