using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;

namespace Foundatio.Repositories.Elasticsearch.Tests {
    public interface IIdentityRepository : IQueryableRepository<Identity> { }

    public class IdentityRepository : ElasticRepositoryBase<Identity>, IIdentityRepository {
        public IdentityRepository(MyAppElasticConfiguration configuration) : base(configuration.Identities) {}
    }

    public class IdentityWithNoCachingRepository : IdentityRepository {
        public IdentityWithNoCachingRepository(MyAppElasticConfiguration configuration) : base(configuration) {
            DisableCache();
        }
    }
}