using System;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Models;

namespace Foundatio.Repositories.Elasticsearch.Tests.Configuration {
    public class LogEventIndex : MonthlyIndex {
        public LogEventIndex() : base(name: "logevents", version: 1) {
            LogEvent = new LogEventType(this);
            AddIndexType<LogEvent>(LogEvent);
        }

        public LogEventType LogEvent { get; }
    }
}