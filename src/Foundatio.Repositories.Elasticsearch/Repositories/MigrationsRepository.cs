using System;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Migrations;
using Nest;

namespace Foundatio.Repositories.Elasticsearch {
    public class MigrationRepository : ElasticRepositoryBase<Migration>, IMigrationRepository {
        public MigrationRepository(IIndexType<Migration> indexType) : base(indexType) {}
    }

    public sealed class MigrationType : IndexTypeBase<Migration> {
        public MigrationType(IIndex index) : base(index) {}

        public override PutMappingDescriptor<Migration> BuildMapping(PutMappingDescriptor<Migration> map) {
            return base.BuildMapping(map).Dynamic();
        }
    }
}
