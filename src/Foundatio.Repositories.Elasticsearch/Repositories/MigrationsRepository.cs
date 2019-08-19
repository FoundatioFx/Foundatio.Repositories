using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Migrations;

namespace Foundatio.Repositories.Elasticsearch {
    public class MigrationRepository : ElasticRepositoryBase<Migration>, IMigrationRepository {
        public MigrationRepository(MigrationIndex index) : base(index) {}
    }

    public sealed class MigrationIndex : Index<Migration> {
        public MigrationIndex(IElasticConfiguration configuration, string name = "migration") : base(configuration, name) {}
    }
}
