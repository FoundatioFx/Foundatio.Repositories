using System;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Types;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Indexes {
    public sealed class MonthlyLogEventIndex : MonthlyIndex {
        public MonthlyLogEventIndex(IElasticConfiguration configuration) : base(configuration, "monthly-logevents", 1) {
            AddType(LogEvent = new LogEventType(this));
            AddAlias($"{Name}-thismonth", TimeSpan.FromDays(32));
            AddAlias($"{Name}-last3months", TimeSpan.FromDays(100));
        }

        public LogEventType LogEvent { get; }
    }
}