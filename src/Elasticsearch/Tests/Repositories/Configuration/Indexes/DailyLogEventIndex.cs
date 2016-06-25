using System;
using Foundatio.Repositories.Elasticsearch.Configuration;

namespace Foundatio.Repositories.Elasticsearch.Tests.Configuration {
    public class DailyLogEventIndex : DailyIndex {
        public DailyLogEventIndex() : base(name: "daily-logevents", version: 1) {
            LogEvent = new LogEventType(this);
            IndexTypes.Add(LogEvent);
        }

        public LogEventType LogEvent { get; }
    }
}