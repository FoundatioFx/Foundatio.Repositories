using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Types {
    public class LogEventType : TimeSeriesIndexType<LogEvent> {
        public LogEventType(IIndex index) : base(index) { }

        public override PutMappingDescriptor<LogEvent> BuildMapping(PutMappingDescriptor<LogEvent> map) {
            return map
                .Type(Name)
                .Dynamic(false)
                .Properties(p => p
                    .String(f => f.Name(e => e.Id).IndexName(Fields.Id).Index(FieldIndexOption.NotAnalyzed))
                    .String(f => f.Name(e => e.CompanyId).IndexName(Fields.CompanyId).Index(FieldIndexOption.NotAnalyzed))
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