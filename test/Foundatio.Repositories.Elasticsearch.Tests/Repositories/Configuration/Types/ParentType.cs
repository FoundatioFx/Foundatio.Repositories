using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Types {
    public class ParentType : IndexTypeBase<Parent> {
        public ParentType(IIndex index): base(index) {}

        public override TypeMappingDescriptor<object> ConfigureProperties(TypeMappingDescriptor<object> map) {
            return base.ConfigureProperties(map
                .Properties<Parent>(p => p
                    .Join(j => j.Name(n => n.JoinField).Relations(r => r.Join<Parent, Child>()))
                ));
        }
    }
}
