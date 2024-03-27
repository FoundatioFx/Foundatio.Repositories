using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Migrations;
using Nest;

namespace Foundatio.Repositories.Elasticsearch;

public class MigrationStateRepository : ElasticRepositoryBase<MigrationState>, IMigrationStateRepository
{
    public MigrationStateRepository(MigrationIndex index) : base(index)
    {
        DisableCache();
        DefaultConsistency = Consistency.Immediate;
    }
}

public class MigrationIndex : Index<MigrationState>
{
    private readonly int _replicas;

    public MigrationIndex(IElasticConfiguration configuration, string name = "migration", int replicas = 1) : base(configuration, name)
    {
        _replicas = replicas;
    }

    public override TypeMappingDescriptor<MigrationState> ConfigureIndexMapping(TypeMappingDescriptor<MigrationState> map)
    {
        return map
            .Dynamic(false)
            .Properties(p => p
                .Keyword(f => f.Name(e => e.Id))
                .Number(f => f.Name(e => e.Version).Type(NumberType.Integer))
                .Date(f => f.Name(e => e.StartedUtc))
                .Date(f => f.Name(e => e.CompletedUtc))
            );
    }

    public override CreateIndexDescriptor ConfigureIndex(CreateIndexDescriptor idx)
    {
        return base.ConfigureIndex(idx).Settings(s => s
            .NumberOfShards(1)
            .NumberOfReplicas(_replicas));
    }
}
