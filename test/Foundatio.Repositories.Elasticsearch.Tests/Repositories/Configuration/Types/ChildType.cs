using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Types {
    public class ChildType : ChildIndexType<Child, Parent> {
        public ChildType(IIndex index): base(d => d.ParentId, index) {}

        public override TypeMappingDescriptor<object> ConfigureProperties(TypeMappingDescriptor<object> map) {
            return base.ConfigureProperties(map
                .Properties<Child>(p => p
                    .Join(j => j.Name(n => n.JoinField).Relations(r => r.Join<Parent, Child>()))
                    .Keyword(c => c.Name(f => f.ParentId))
                ));
        }
    }
}
