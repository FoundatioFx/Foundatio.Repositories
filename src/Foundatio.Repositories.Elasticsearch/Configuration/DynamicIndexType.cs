using System;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Configuration {
    public class DynamicIndexType<T> : IndexTypeBase<T> where T : class {
        public DynamicIndexType(IIndex index, string name = null): base(index, name) {}

        public override IPromise<IMappings> BuildMapping(MappingsDescriptor map) {
            return map.Map<T>(Name, d => d.Dynamic().Properties(p => p.SetupDefaults()));
        }
    }
}