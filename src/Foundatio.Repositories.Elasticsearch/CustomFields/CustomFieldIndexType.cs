using System.Threading.Tasks;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.CustomFields;

public interface ICustomFieldIndexType<T> where T : class {
    string Type { get; }
    Task<object> ToIdxValueAsync(object value);
    IProperty ConfigureMapping(SingleMappingSelector<T> map);
}

public class StringCustomFieldType<T> : ICustomFieldIndexType<T> where T : class {
    public string Type => "string";

    public Task<object> ToIdxValueAsync(object value) {
        return Task.FromResult(value);
    }

    public virtual IProperty ConfigureMapping(SingleMappingSelector<T> map) {
        return map.Text(mp => mp.AddKeywordField());
    }
}
