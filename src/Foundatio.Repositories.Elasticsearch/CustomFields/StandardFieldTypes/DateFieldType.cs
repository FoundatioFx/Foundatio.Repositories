using System;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch.Mapping;

namespace Foundatio.Repositories.Elasticsearch.CustomFields;

public class DateFieldType : ICustomFieldType
{
    public static string IndexType = "date";
    public string Type => IndexType;

    public virtual Task<ProcessFieldValueResult> ProcessValueAsync<T>(T document, object value, CustomFieldDefinition fieldDefinition) where T : class
    {
        return Task.FromResult(new ProcessFieldValueResult { Value = value });
    }

    public virtual Func<PropertyFactory<T>, IProperty> ConfigureMapping<T>() where T : class
    {
        return factory => factory.Date();
    }
}
