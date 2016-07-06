using System;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Tests.Configuration {
    public class MonthlyLogEventIndex : MonthlyIndex {
        public MonthlyLogEventIndex(IElasticClient client) : base(client, name: "monthly-logevents", version: 1) {
            LogEvent = new LogEventType(this);
            AddType(LogEvent);
        }

        public LogEventType LogEvent { get; }
    }
}