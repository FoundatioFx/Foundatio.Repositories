using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Migrations;

namespace Foundatio.Repositories.Elasticsearch {
    public class MigrationRepository : ElasticRepositoryBase<Migration>, IMigrationRepository {
        public MigrationRepository(IIndexType<Migration> indexType) : base(indexType) {}
    }

    public sealed class MigrationType : DynamicIndexType<Migration> {
        public MigrationType(IIndex index) : base(index) {}
    }
}
