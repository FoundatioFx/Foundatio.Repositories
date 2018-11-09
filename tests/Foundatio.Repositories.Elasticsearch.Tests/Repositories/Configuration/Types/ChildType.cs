using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Types {
    public class ChildType : ChildIndexType<Child, Parent> {
        public ChildType(IIndex index): base(d => d.ParentId, index) {}

        public override TypeMappingDescriptor<Child> BuildMapping(TypeMappingDescriptor<Child> map) {
            return base.BuildMapping(map
                .Parent<Parent>()
                .Properties(p => p
                    .Keyword(c => c.Name(f => f.ParentId))
                ));
        }
    }
}
