using System;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Migrations;

namespace Foundatio.Repositories.Elasticsearch {
    public class MigrationsRepository : ElasticRepositoryBase<Migration>, IMigrationRepository {
        public MigrationsRepository(IElasticConfiguration elasticConfiguration) : base(elasticConfiguration) {}
    }
}
