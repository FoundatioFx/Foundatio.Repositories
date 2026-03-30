using System;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Indexes;

public sealed class MonthlyLogEventIndex : MonthlyIndex<LogEvent>
{
    public MonthlyLogEventIndex(IElasticConfiguration configuration) : base(configuration, "monthly-logevents", 1)
    {
        AddAlias($"{Name}-thismonth", TimeSpan.FromDays(32));
        AddAlias($"{Name}-last3months", TimeSpan.FromDays(100));
    }

    public override void ConfigureIndex(CreateIndexRequestDescriptor idx)
    {
        base.ConfigureIndex(idx.Settings(s => s.NumberOfReplicas(0).NumberOfShards(1)));
    }
}
