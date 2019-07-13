using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Indexes {
    public sealed class IdentityIndex : Index<Identity> {
        public IdentityIndex(IElasticConfiguration configuration) : base(configuration, "identity") {}

        public override CreateIndexDescriptor ConfigureIndex(CreateIndexDescriptor idx) {
            return base.ConfigureIndex(idx.Settings(s => s.NumberOfReplicas(0).NumberOfShards(1)));
        }

        public override TypeMappingDescriptor<Identity> ConfigureIndexMapping(TypeMappingDescriptor<Identity> map) {
            return map
                .Dynamic(false)
                .Properties(p => p
                    .Keyword(f => f.Name(e => e.Id))
                );
        }
    }
}