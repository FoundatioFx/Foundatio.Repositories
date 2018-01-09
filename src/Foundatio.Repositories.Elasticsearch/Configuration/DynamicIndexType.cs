using Nest;

namespace Foundatio.Repositories.Elasticsearch.Configuration {
    public class DynamicIndexType<T> : IndexTypeBase<T> where T : class {
        public DynamicIndexType(IIndex index): base(index) {}

        public override TypeMappingDescriptor<object> ConfigureProperties(TypeMappingDescriptor<object> map) {
            return base.ConfigureProperties(map.Dynamic(true));
        }
    }
}