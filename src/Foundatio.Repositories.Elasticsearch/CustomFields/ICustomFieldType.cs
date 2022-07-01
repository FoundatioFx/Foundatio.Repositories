using System.Threading.Tasks;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.CustomFields;

public interface ICustomFieldType {
    string Type { get; }
    Task<object> TransformToIdxAsync(object value);
    IProperty ConfigureMapping<T>(SingleMappingSelector<T> map) where T: class;
}
