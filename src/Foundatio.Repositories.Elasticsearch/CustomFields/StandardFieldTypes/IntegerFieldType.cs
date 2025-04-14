using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch.Mapping;

namespace Foundatio.Repositories.Elasticsearch.CustomFields;

public class IntegerFieldType : ICustomFieldType
{
    public static string IndexType = "int";
    public string Type => IndexType;

    public virtual Task<ProcessFieldValueResult> ProcessValueAsync<T>(T document, object value, CustomFieldDefinition fieldDefinition) where T : class
    {
        return Task.FromResult(new ProcessFieldValueResult { Value = value });
    }

    public virtual IProperty ConfigureMapping<T>(SingleMappingSelector<T> map) where T : class
    {
        return map.Number(mp => mp.Type(NumberType.Integer));
    }
}
