using System;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch.Mapping;
using Foundatio.Parsers.ElasticQueries.Extensions;

namespace Foundatio.Repositories.Elasticsearch.CustomFields;

/// <summary>
/// Maps a custom field as a text field with a <c>.keyword</c> sub-field for exact matching.
/// The <see cref="ConfigureMapping{T}"/> call to <c>AddKeywordField()</c> is required by
/// the NEST beta for Elasticsearch 8.x+. Without it, text fields lack the <c>.keyword</c>
/// sub-field needed for aggregations, sorting, and exact-match filtering.
/// </summary>
public class StringFieldType : ICustomFieldType
{
    public static string IndexType = "string";
    public string Type => IndexType;

    public virtual Task<ProcessFieldValueResult> ProcessValueAsync<T>(T document, object? value, CustomFieldDefinition fieldDefinition) where T : class
    {
        return Task.FromResult(new ProcessFieldValueResult { Value = value });
    }

    public virtual Func<PropertyFactory<T>, IProperty> ConfigureMapping<T>() where T : class
    {
        return factory => factory.Text(p => p.AddKeywordField());
    }
}
