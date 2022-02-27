using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public interface IIdentityRepository : ISearchableRepository<Identity> { }

public class IdentityRepository : ElasticRepositoryBase<Identity>, IIdentityRepository {
    public IdentityRepository(MyAppElasticConfiguration configuration) : base(configuration.Identities) {}
}

public class IdentityWithNoCachingRepository : IdentityRepository {
    public IdentityWithNoCachingRepository(MyAppElasticConfiguration configuration) : base(configuration) {
        DisableCache();
    }
}
