using System;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Types {
    public class IdentityType : IndexTypeBase<Identity> {
        public IdentityType(IIndex index) : base(index) { }

        public override PutMappingDescriptor<Identity> BuildMapping(PutMappingDescriptor<Identity> map) {
            return map
                .Type(Name)
                .Dynamic(false)
                .Properties(p => p
                    .String(f => f.Name(e => e.Id).IndexName(Fields.Id).Index(FieldIndexOption.NotAnalyzed))
                );
        }

        public class Fields {
            public const string Id = "id";
        }
    }
}