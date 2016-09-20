using System;
using Foundatio.Repositories.Elasticsearch.Tests.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Models;

namespace Foundatio.Repositories.Elasticsearch.Tests {
    public class IdentityRepository : ElasticRepositoryBase<Identity> {
        public IdentityRepository(MyAppElasticConfiguration configuration) : base(configuration.Identities.Identity) {}
    }

    public class IdentityWithNoCachingRepository : IdentityRepository {
        public IdentityWithNoCachingRepository(MyAppElasticConfiguration configuration) : base(configuration) {
            DisableCache();
        }
    }
}