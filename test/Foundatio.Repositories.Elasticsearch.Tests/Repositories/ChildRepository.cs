using System.Threading.Tasks;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories {
    public class ChildRepository : ElasticRepositoryBase<Child> {
        public ChildRepository(MyAppElasticConfiguration elasticConfiguration) : base(elasticConfiguration.ParentChild.Child) {
        }

        public Task<FindResults<Child>> QueryAsync(RepositoryQueryDescriptor<Child> query, CommandOptionsDescriptor<Child> options = null) {
            return FindAsync(query, options);
        }
    }
}
