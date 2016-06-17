using System;
using FluentValidation;
using Foundatio.Caching;
using Foundatio.Messaging;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Migrations;
using Nest;

namespace Foundatio.Repositories.Elasticsearch {
    public class MigrationsRepository : ElasticRepositoryBase<MigrationResult>, IMigrationRepository {
        public MigrationsRepository(IElasticClient client) : base(new MigrationRespositoryConfiguration(client)) {}
    }

    public class MigrationRespositoryConfiguration : ElasticRepositoryConfiguration<MigrationResult> {
        public MigrationRespositoryConfiguration(IElasticClient client): base(client, new ElasticIndexType<MigrationResult>("migrations")) {}
    }
}
