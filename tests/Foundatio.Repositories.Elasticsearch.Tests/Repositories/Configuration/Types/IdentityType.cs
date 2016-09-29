using System;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Models;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Tests.Configuration {
    public class IdentityType : IndexTypeBase<Identity> {
        public IdentityType(IIndex index) : base(index) { }

        public override TypeMappingDescriptor<Identity> BuildMapping(TypeMappingDescriptor<Identity> map) {
            return map
                .Dynamic(false)
                .Properties(p => p
                    .Keyword(f => f.Name(e => e.Id))
                );
        }
    }
}