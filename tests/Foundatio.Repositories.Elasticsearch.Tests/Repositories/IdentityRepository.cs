using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;

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