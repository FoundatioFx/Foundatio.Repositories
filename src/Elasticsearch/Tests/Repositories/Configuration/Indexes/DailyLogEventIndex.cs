using System;
using Foundatio.Repositories.Elasticsearch.Configuration;

namespace Foundatio.Repositories.Elasticsearch.Tests.Configuration {
    public sealed class DailyLogEventIndex : DailyIndex {
        public DailyLogEventIndex(IElasticConfiguration elasticConfiguration) : base(elasticConfiguration, "daily-logevents", 1) {
            AddType(LogEvent = new LogEventType(this));
            AddAlias($"{Name}-today", TimeSpan.FromDays(1));
            AddAlias($"{Name}-last7days", TimeSpan.FromDays(7));
            AddAlias($"{Name}-last30days", TimeSpan.FromDays(30));
        }

        public LogEventType LogEvent { get; }
    }
}