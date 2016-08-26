using System;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Types {
    public class ChildType : ChildIndexType<Child, Parent> {
        public ChildType(IIndex index = null): base("parent", "parentId", d => d.ParentId, null, index) {}

        public override PutMappingDescriptor<Child> BuildMapping(PutMappingDescriptor<Child> map) {
            return base.BuildMapping(map
                .SetParent<Parent>()
                .Properties(p => p
                    .String(c => c.Name(f => f.ParentId).Index(FieldIndexOption.NotAnalyzed))
                ));
        }
    }
}
