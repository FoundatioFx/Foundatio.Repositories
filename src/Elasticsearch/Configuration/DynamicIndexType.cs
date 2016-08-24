using System;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Configuration {
    public sealed class DynamicIndexType<T> : IndexTypeBase<T> where T : class {
        public DynamicIndexType(IIndex index, string name = null): base(index, name) {}
        
        public override PutMappingDescriptor<T> BuildMapping(PutMappingDescriptor<T> map) {
            return base.BuildMapping(map.Dynamic());
        }
    }
}
