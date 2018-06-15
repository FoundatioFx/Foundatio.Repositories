using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Migrations;

namespace Foundatio.Repositories.Elasticsearch {
    public class MigrationRepository : ElasticRepositoryBase<Migration>, IMigrationRepository {
        public MigrationRepository(IIndex<Migration> indexType) : base(indexType) {}
    }

    public sealed class MigrationIndex : DynamicIndex<Migration> {
        public MigrationIndex(IElasticConfiguration configuration) : base(configuration) {}
    }
}
