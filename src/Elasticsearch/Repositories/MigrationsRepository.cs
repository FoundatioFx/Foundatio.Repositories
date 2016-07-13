using System;
using Foundatio.Logging;
using Foundatio.Repositories.Migrations;
using Nest;

namespace Foundatio.Repositories.Elasticsearch {
    public class MigrationsRepository : ElasticRepositoryBase<Migration>, IMigrationRepository {
        public MigrationsRepository(IElasticClient client, ILogger<MigrationsRepository> logger) : base(client, null, null, null, logger) {}
    }
}
