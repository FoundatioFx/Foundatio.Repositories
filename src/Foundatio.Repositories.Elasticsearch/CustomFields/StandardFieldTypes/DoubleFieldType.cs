using System.Threading.Tasks;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.CustomFields;

public class DoubleFieldType : ICustomFieldType {
    public static string IndexType = "double";
    public string Type => IndexType;

    public Task<ProcessFieldValueResult> ProcessValueAsync<T>(T document, object value, CustomFieldDefinition fieldDefinition) where T : class {
        return Task.FromResult(new ProcessFieldValueResult { Value = value, Idx = value });
    }

    public virtual IProperty ConfigureMapping<T>(SingleMappingSelector<T> map) where T : class {
        return map.Number(mp => mp.Type(NumberType.Double));
    }
}
