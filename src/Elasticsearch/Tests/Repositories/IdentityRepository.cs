using System;
using Foundatio.Repositories.Elasticsearch.Tests.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Models;

namespace Foundatio.Repositories.Elasticsearch.Tests {
    public class IdentityRepository : ElasticRepositoryBase<Identity> {
        public IdentityRepository(MyAppElasticConfiguration elasticConfiguration) : base(elasticConfiguration.Identities.Identity) {
        }

        public IdentityRepository( IdentityType identityType) : base(identityType) {
        }
    }
}