using System.Threading.Tasks;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.CustomFields;

public class IntegerFieldType : ICustomFieldType {
    public static string IndexType = "int";
    public string Type => IndexType;

    public Task<object> TransformToIdxAsync(object value) {
        return Task.FromResult(value);
    }

    public virtual IProperty ConfigureMapping<T>(SingleMappingSelector<T> map) where T : class {
        return map.Number(mp => mp.Type(NumberType.Integer));
    }
}
