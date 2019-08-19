using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Migrations;
using Nest;

namespace Foundatio.Repositories.Elasticsearch {
    public class MigrationRepository : ElasticRepositoryBase<Migration>, IMigrationRepository {
        public MigrationRepository(MigrationIndex index) : base(index) {}
    }

    public class MigrationIndex : Index<Migration> {
        private readonly int _replicas;

        public MigrationIndex(IElasticConfiguration configuration, string name = "migration", int replicas = 1) : base(configuration, name) {
            _replicas = replicas;
        }

        public override CreateIndexDescriptor ConfigureIndex(CreateIndexDescriptor idx) {
            return base.ConfigureIndex(idx).Settings(s => s
                .NumberOfShards(1)
                .NumberOfReplicas(_replicas));
        }
    }
}
