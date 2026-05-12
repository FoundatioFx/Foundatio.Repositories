using System;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Clients.Elasticsearch.Mapping;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Queries;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Indexes;

public sealed class DailyLogEventIndex : DailyIndex<LogEvent>
{
    public DailyLogEventIndex(IElasticConfiguration configuration) : base(configuration, "daily-logevents", 1, doc => ((LogEvent)doc).Date.UtcDateTime)
    {
        AddAlias($"{Name}-today", TimeSpan.FromDays(1));
        AddAlias($"{Name}-last7days", TimeSpan.FromDays(7));
        AddAlias($"{Name}-last30days", TimeSpan.FromDays(30));
    }

    public override void ConfigureIndexMapping(TypeMappingDescriptor<LogEvent> map)
    {
        map
            .Dynamic(DynamicMapping.False)
            .Properties(p => p
                .SetupDefaults()
                .Keyword(e => e.CompanyId)
                .Date(e => e.Date)
            );
    }

    protected override void ConfigureQueryBuilder(ElasticQueryBuilder builder)
    {
        builder.Register<CompanyQueryBuilder>();
    }

    public override void ConfigureIndex(CreateIndexRequestDescriptor idx)
    {
        base.ConfigureIndex(idx.Settings(s => s.NumberOfReplicas(0).NumberOfShards(1)));
    }
}
