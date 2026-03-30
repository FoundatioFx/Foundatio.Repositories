using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Clients.Elasticsearch.Mapping;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.SampleApp.Shared;

namespace Foundatio.SampleApp.Server.Repositories.Indexes;

public sealed class GameReviewIndex : VersionedIndex<GameReview>
{
    public GameReviewIndex(IElasticConfiguration configuration) : base(configuration, "gamereview", version: 1) { }

    public override void ConfigureIndex(CreateIndexRequestDescriptor idx)
    {
        base.ConfigureIndex(idx);
        idx.Settings(s => s
            .Analysis(a => a.AddSortNormalizer())
            .NumberOfReplicas(0)
            .NumberOfShards(1));
    }

    public override void ConfigureIndexMapping(TypeMappingDescriptor<GameReview> map)
    {
        // adding new fields will automatically update the index mapping
        // changing existing fields requires a new index version and a reindex
        map
            .Dynamic(DynamicMapping.False)
            .Properties(p => p
                .SetupDefaults()
                .Text(e => e.Name, f => f.AddKeywordAndSortFields())
                .Text(e => e.Description)
                .Text(e => e.Category, f => f.AddKeywordAndSortFields())
                .Text(e => e.Tags, f => f.AddKeywordAndSortFields())
            );
    }
}
