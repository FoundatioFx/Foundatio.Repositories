using System.Threading.Tasks;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories {
    public class ParentRepository : ElasticRepositoryBase<Parent> {
        public ParentRepository(MyAppElasticConfiguration elasticConfiguration) : base(elasticConfiguration.ParentChild.Parent) {
        }

        public Task<FindResults<Parent>> QueryAsync(RepositoryQueryDescriptor<Parent> query) {
            return FindAsync(query);
        }
    }
}
