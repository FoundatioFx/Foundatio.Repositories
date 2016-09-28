using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Models;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Tests.Configuration {
    public class LogEventType : TimeSeriesIndexType<LogEvent> {
        public LogEventType(IIndex index) : base(index) { }

        public override TypeMappingDescriptor<LogEvent> BuildMapping(TypeMappingDescriptor<LogEvent> map) {
            return map
                .Dynamic(false)
                .Properties(p => p
                    .Keyword(f => f.Name(e => e.Id).IndexName(Fields.Id))
                    .Keyword(f => f.Name(e => e.CompanyId).IndexName(Fields.CompanyId))
                    .Date(f => f.Name(e => e.CreatedUtc).IndexName(Fields.CreatedUtc))
                );
        }

        public class Fields {
            public const string Id = "id";
            public const string CompanyId = "company";
            public const string CreatedUtc = "created";
        }
    }
}