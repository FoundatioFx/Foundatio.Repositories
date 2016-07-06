using System;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Tests.Configuration {
    public class DailyLogEventIndex : DailyIndex {
        public DailyLogEventIndex(IElasticClient client) : base(client, name: "daily-logevents", version: 1) {
            LogEvent = new LogEventType(this);
            AddType(LogEvent);
        }

        public LogEventType LogEvent { get; }
    }
}