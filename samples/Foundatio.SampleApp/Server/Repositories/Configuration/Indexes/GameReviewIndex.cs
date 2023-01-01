using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.SampleApp.Shared;
using Nest;

namespace Foundatio.SampleApp.Server.Repositories.Indexes;

public sealed class GameReviewIndex : VersionedIndex<GameReview> {
    public GameReviewIndex(IElasticConfiguration configuration) : base(configuration, "gamereview", version: 1) {}

    public override CreateIndexDescriptor ConfigureIndex(CreateIndexDescriptor idx) {
        return base.ConfigureIndex(idx.Settings(s => s
            .Analysis(a => a.AddSortNormalizer())
            .NumberOfReplicas(0)
            .NumberOfShards(1)));
    }

    public override TypeMappingDescriptor<GameReview> ConfigureIndexMapping(TypeMappingDescriptor<GameReview> map) {
        // adding new fields will automatically update the index mapping
        // changing existing fields requires a new index version and a reindex
        return map
            .Dynamic(false)
            .Properties(p => p
                .SetupDefaults()
                .Text(f => f.Name(e => e.Name).AddKeywordAndSortFields())
                .Text(f => f.Name(e => e.Description))
                .Text(f => f.Name(e => e.Category).AddKeywordAndSortFields())
                .Text(f => f.Name(e => e.Tags).AddKeywordAndSortFields())
            );
    }
}
