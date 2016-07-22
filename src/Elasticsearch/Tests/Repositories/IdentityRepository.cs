using System;
using Foundatio.Caching;
using Foundatio.Logging;
using Foundatio.Repositories.Elasticsearch.Tests.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Models;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Tests {
    public class IdentityRepository : ElasticRepositoryBase<Identity> {
        public IdentityRepository(MyAppElasticConfiguration elasticConfiguration, ICacheClient cache, ILogger<IdentityRepository> logger) : base(elasticConfiguration.Client, null, cache, null, logger) {
            ElasticType = elasticConfiguration.Identities.Identity;
        }

        public IdentityRepository(IElasticClient client, IdentityType identityType, ICacheClient cache, ILogger<IdentityRepository> logger) : base(client, null, cache, null, logger) {
            ElasticType = identityType;
        }
    }
}