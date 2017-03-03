using Nest;

namespace Foundatio.Repositories.Elasticsearch.Configuration {
    public class DynamicIndexType<T> : IndexTypeBase<T> where T : class {
        public DynamicIndexType(IIndex index, string name = null): base(index, name) {}

        public override TypeMappingDescriptor<T> BuildMapping(TypeMappingDescriptor<T> map) {
            return base.BuildMapping(map.Dynamic(true));
        }
    }
}