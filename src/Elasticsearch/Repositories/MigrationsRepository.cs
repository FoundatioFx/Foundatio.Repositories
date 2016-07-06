using System;
using Foundatio.Logging;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Migrations;
using Nest;

namespace Foundatio.Repositories.Elasticsearch {
    public class MigrationsRepository : ElasticRepositoryBase<MigrationResult>, IMigrationRepository {
        public MigrationsRepository(IElasticClient client) : base(new MigrationRespositoryConfiguration(client)) {}
    }

    public class MigrationRespositoryConfiguration : RepositoryConfiguration<MigrationResult> {
        public MigrationRespositoryConfiguration(IElasticClient client): base(client, new MigrationIndex(client, "migrations").MigrationType) {}
    }

    public class MigrationIndex : Index {
        public MigrationIndex(IElasticClient client, string name, ILoggerFactory loggerFactory = null): base(client, name, loggerFactory) {
            MigrationType = new IndexType<MigrationResult>(this, "migrations");
            AddType(MigrationType);
        }

        public IndexType<MigrationResult> MigrationType { get; }
    }
}
