using System;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Configuration {
    public sealed class DynamicIndexType : IndexTypeBase<object> {
        public DynamicIndexType(IIndex index, string name = null): base(index, name) {}

        public override PutMappingDescriptor<object> BuildMapping(PutMappingDescriptor<object> map) {
            return base.BuildMapping(map.Dynamic());
        }
    }
}
