using System.Threading.Tasks;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.CustomFields;

public class DateFieldType : ICustomFieldType
{
    public static string IndexType = "date";
    public string Type => IndexType;

    public virtual Task<ProcessFieldValueResult> ProcessValueAsync<T>(T document, object value, CustomFieldDefinition fieldDefinition) where T : class
    {
        return Task.FromResult(new ProcessFieldValueResult { Value = value });
    }

    public virtual IProperty ConfigureMapping<T>(SingleMappingSelector<T> map) where T : class
    {
        return map.Date(mp => mp);
    }
}
