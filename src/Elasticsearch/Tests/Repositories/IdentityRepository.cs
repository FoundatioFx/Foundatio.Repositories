using System;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Models;

namespace Foundatio.Repositories.Elasticsearch.Tests {
    public class IdentityRepository : ElasticRepositoryBase<Identity> {
        public IdentityRepository(MyAppElasticConfiguration elasticConfiguration) : base(elasticConfiguration) {
            ElasticType = elasticConfiguration.Identities.Identity;
        }

        public IdentityRepository(IElasticConfiguration elasticConfiguration, IdentityType identityType) : base(elasticConfiguration) {
            ElasticType = identityType;
        }
    }
}