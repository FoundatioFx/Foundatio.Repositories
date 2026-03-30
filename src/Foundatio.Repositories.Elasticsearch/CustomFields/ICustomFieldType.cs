using System;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch.Mapping;

namespace Foundatio.Repositories.Elasticsearch.CustomFields;

/// <summary>
/// Defines a custom field type that controls how values are processed during document save
/// and how the Elasticsearch mapping is configured for the field's index slots.
/// </summary>
public interface ICustomFieldType
{
    /// <summary>
    /// Gets the unique type identifier (e.g., <c>"string"</c>, <c>"int"</c>, <c>"bool"</c>).
    /// This must match the <see cref="CustomFieldDefinition.IndexType"/> of definitions using this type.
    /// </summary>
    string Type { get; }

    /// <summary>
    /// Processes a custom field value during document save. Called for each custom field that matches this type.
    /// Override to transform, validate, or compute values.
    /// </summary>
    /// <typeparam name="T">The document type.</typeparam>
    /// <param name="document">The document being saved.</param>
    /// <param name="value">The current value from <c>Data</c> (may be <c>null</c> for <see cref="CustomFieldProcessMode.AlwaysProcess"/> fields).</param>
    /// <param name="fieldDefinition">The custom field definition for this field.</param>
    /// <returns>A <see cref="ProcessFieldValueResult"/> containing the processed value.</returns>
    Task<ProcessFieldValueResult> ProcessValueAsync<T>(T document, object value, CustomFieldDefinition fieldDefinition) where T : class;

    /// <summary>
    /// Configures the Elasticsearch mapping for index slots of this type.
    /// This is used in dynamic templates to map <c>idx.{type}-*</c> fields.
    /// </summary>
    Func<PropertyFactory<T>, IProperty> ConfigureMapping<T>() where T : class;
}

/// <summary>
/// Contains the result of processing a custom field value via <see cref="ICustomFieldType.ProcessValueAsync{T}"/>.
/// </summary>
public class ProcessFieldValueResult
{
    /// <summary>
    /// The processed value to store back in the document's <c>Data</c> dictionary.
    /// </summary>
    public object Value { get; set; }

    /// <summary>
    /// An optional separate value to store in the <c>Idx</c> dictionary for indexing.
    /// When <c>null</c>, <see cref="Value"/> is used for both storage and indexing.
    /// </summary>
    public object Idx { get; set; }

    /// <summary>
    /// Set to <c>true</c> if the processor modified the <see cref="CustomFieldDefinition"/> itself
    /// (e.g., updated metadata in <see cref="CustomFieldDefinition.Data"/>). This triggers a save of the definition.
    /// </summary>
    public bool IsCustomFieldDefinitionModified { get; set; }
}
