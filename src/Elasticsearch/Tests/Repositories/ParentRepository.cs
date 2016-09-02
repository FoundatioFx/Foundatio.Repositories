using System;
using System.Threading.Tasks;
using Foundatio.Repositories.Elasticsearch.Tests.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Queries;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories {
    public class ParentRepository : ElasticRepositoryBase<Parent> {
        public ParentRepository(MyAppElasticConfiguration elasticConfiguration) : base(elasticConfiguration) {
            ElasticType = elasticConfiguration.ParentChild.Parent;
        }

        public Task<IFindResults<Parent>> QueryAsync(IRepositoryQuery query) {
            return FindAsync(query);
        }
    }
}
