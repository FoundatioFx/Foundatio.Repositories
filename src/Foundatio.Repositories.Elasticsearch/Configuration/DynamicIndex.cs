using Foundatio.Repositories.Elasticsearch.Extensions;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Configuration {
    public class DynamicIndex<T> : Index<T> where T : class {
        public DynamicIndex(IElasticConfiguration configuration, string name = null): base(configuration, name) {}

        public override ITypeMapping ConfigureIndexMapping(TypeMappingDescriptor<T> map) {
            return map.Dynamic().AutoMap<T>().Properties(p => p.SetupDefaults());
        }
    }
}