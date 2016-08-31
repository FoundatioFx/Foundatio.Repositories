using System;
using System.Threading.Tasks;
using Foundatio.Caching;
using Foundatio.Logging;
using Foundatio.Repositories.Elasticsearch.Tests.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Queries;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories {
    public class ParentRepository : ElasticRepositoryBase<Parent> {
        public ParentRepository(MyAppElasticConfiguration elasticConfiguration, ICacheClient cache, ILogger<ParentRepository> logger) : base(elasticConfiguration.Client, null, cache, null, logger) {
            ElasticType = elasticConfiguration.ParentChild.Parent;
        }

        public Task<IFindResults<Parent>> QueryAsync(IRepositoryQuery query) {
            return FindAsync(query);
        }
    }
}
