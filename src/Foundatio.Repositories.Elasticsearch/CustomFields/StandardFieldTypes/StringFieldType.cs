using System.Threading.Tasks;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.CustomFields;

public class StringFieldType : ICustomFieldType {
    public static string IndexType = "string";
    public string Type => IndexType;

    public Task<object> TransformToIdxAsync(object value) {
        return Task.FromResult(value);
    }

    public virtual IProperty ConfigureMapping<T>(SingleMappingSelector<T> map) where T : class {
        return map.Text(mp => mp.AddKeywordField());
    }
}
