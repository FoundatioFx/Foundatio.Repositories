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
