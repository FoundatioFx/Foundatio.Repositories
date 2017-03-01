using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Types;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Indexes {
    public sealed class IdentityIndex : Index {
        public IdentityIndex(IElasticConfiguration configuration) : base(configuration, "identity") {
            AddType(Identity = new IdentityType(this));
        }

        public IdentityType Identity { get; }

        public override CreateIndexDescriptor ConfigureIndex(CreateIndexDescriptor idx) {
            return base.ConfigureIndex(idx.Settings(s => s.NumberOfReplicas(0).NumberOfShards(1)));
        }
    }
}