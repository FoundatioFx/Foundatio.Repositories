using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Clients.Elasticsearch.Mapping;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Migrations;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories.Elasticsearch;

public class MigrationStateRepository : ElasticRepositoryBase<MigrationState>, IMigrationStateRepository
{
    public MigrationStateRepository(MigrationIndex index) : base(index)
    {
        DisableCache();
        DefaultConsistency = Consistency.Immediate;
    }

    // Preserve upserts for caller-created records. Loaded records carry Elasticsearch's sequence number and primary term.
    private protected override ElasticDocumentVersion? GetVersionForSave(MigrationState document) =>
        string.IsNullOrEmpty(((IVersioned)document).Version) ? null : base.GetVersionForSave(document);
}

public class MigrationIndex : Index<MigrationState>
{
    private readonly int _replicas;

    public MigrationIndex(IElasticConfiguration configuration, string name = "migration", int replicas = 1) : base(configuration, name)
    {
        _replicas = replicas;
    }

    public override void ConfigureIndexMapping(TypeMappingDescriptor<MigrationState> map)
    {
        map
            .Dynamic(DynamicMapping.False)
            .Properties(p => p
                .Keyword(f => f.Id)
                .IntegerNumber(f => f.Version)
                .Date(f => f.StartedUtc)
                .Date(f => f.CompletedUtc)
            );
    }

    public override void ConfigureIndex(CreateIndexRequestDescriptor idx)
    {
        base.ConfigureIndex(idx);
        idx.Settings(s => s
            .NumberOfShards(1)
            .NumberOfReplicas(_replicas));
    }
}
