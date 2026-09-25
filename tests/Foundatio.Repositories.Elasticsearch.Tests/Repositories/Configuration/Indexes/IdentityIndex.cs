using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Clients.Elasticsearch.Mapping;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Indexes;

public sealed class IdentityIndex : Index<Identity>
{
    public IdentityIndex(IElasticConfiguration configuration) : base(configuration, "identity") { }

    public override void ConfigureIndex(CreateIndexRequestDescriptor idx)
    {
        base.ConfigureIndex(idx.Settings(s => s.NumberOfReplicas(0).NumberOfShards(1)));
    }

    public override void ConfigureIndexMapping(TypeMappingDescriptor<Identity> map)
    {
        map
            .Dynamic(DynamicMapping.False)
            .Properties(p => p
                .SetupDefaults()
            );
    }
}

/// <summary>
/// A versioned index whose model has no date fields, so <c>GetTimeStampField()</c> returns null.
/// </summary>
/// <remarks>
/// This is the configuration a reindex cannot catch up on by time-slicing the source, and when its ids are not
/// ObjectIds it cannot slice by id either. That combination is legitimate and supported - not every model has
/// or wants an <c>updated_utc</c> - so it needs a fixture of its own rather than being treated as a
/// misconfiguration of <see cref="VersionedEmployeeIndex"/>.
/// </remarks>
public sealed class VersionedIdentityIndex : VersionedIndex<Identity>
{
    public VersionedIdentityIndex(IElasticConfiguration configuration, int version) : base(configuration, "identity", version) { }

    public override void ConfigureIndex(CreateIndexRequestDescriptor idx)
    {
        base.ConfigureIndex(idx.Settings(s => s.NumberOfReplicas(0).NumberOfShards(1)));
    }

    public override void ConfigureIndexMapping(TypeMappingDescriptor<Identity> map)
    {
        map
            .Dynamic(DynamicMapping.False)
            .Properties(p => p
                .SetupDefaults()
            );
    }
}
