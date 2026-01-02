using Elastic.Clients.Elasticsearch.IndexManagement;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Indexes;

public sealed class DailyFileAccessHistoryIndex : DailyIndex<FileAccessHistory>
{
    public DailyFileAccessHistoryIndex(IElasticConfiguration configuration) : base(configuration, "file-access-history-daily", 1, d => ((FileAccessHistory)d).AccessedDateUtc)
    {
    }

    public override void ConfigureIndex(CreateIndexRequestDescriptor idx)
    {
        base.ConfigureIndex(idx.Settings(s => s.NumberOfReplicas(0).NumberOfShards(1)));
    }
}
