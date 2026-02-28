using Elastic.Clients.Elasticsearch.IndexManagement;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Indexes;

public sealed class ParentChildIndex : VersionedIndex
{
    public ParentChildIndex(IElasticConfiguration configuration) : base(configuration, "parentchild", 1) { }

    public override void ConfigureIndex(CreateIndexRequestDescriptor idx)
    {
        base.ConfigureIndex(idx
            .Settings(s => s.NumberOfReplicas(0).NumberOfShards(1))
            .Mappings<IParentChildDocument>(m => m
                //.RoutingField(r => r.Required())
                .Properties(p => p
                    .SetupDefaults()
                    .Join(d => d.Discriminator, j => j
                        .Relations(r => r.Add("parent", new[] { "child" }))
                    )
                )));
    }
}
