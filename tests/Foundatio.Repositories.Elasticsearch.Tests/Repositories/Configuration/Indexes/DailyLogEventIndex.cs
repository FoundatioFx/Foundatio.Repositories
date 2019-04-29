using System;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Queries;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Indexes {
    public sealed class DailyLogEventIndex : DailyIndex<LogEvent> {
        public DailyLogEventIndex(IElasticConfiguration configuration) : base(configuration, "daily-logevents", 1) {
            AddAlias($"{Name}-today", TimeSpan.FromDays(1));
            AddAlias($"{Name}-last7days", TimeSpan.FromDays(7));
            AddAlias($"{Name}-last30days", TimeSpan.FromDays(30));
        }

        public override ITypeMapping ConfigureIndexMapping(TypeMappingDescriptor<LogEvent> map) {
            return map
                .Dynamic(false)
                .Properties(p => p
                    .Keyword(f => f.Name(e => e.Id))
                    .Keyword(f => f.Name(e => e.CompanyId))
                    .Date(f => f.Name(e => e.CreatedUtc))
                );
        }
        
        protected override void ConfigureQueryBuilder(ElasticQueryBuilder builder) {
            builder.Register<CompanyQueryBuilder>();
        }

        public override CreateIndexDescriptor ConfigureIndex(CreateIndexDescriptor idx) {
            return base.ConfigureIndex(idx.Settings(s => s.NumberOfReplicas(0).NumberOfShards(1)));
        }
    }
}