using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Queries;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Types {
    public class LogEventType : TimeSeriesIndexType<LogEvent> {
        public LogEventType(IIndex index) : base(index) { }

        public override TypeMappingDescriptor<LogEvent> BuildMapping(TypeMappingDescriptor<LogEvent> map) {
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
    }
}