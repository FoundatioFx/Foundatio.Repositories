using System;
using Foundatio.Logging;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Tests.Configuration {
    public sealed class DailyLogEventIndex : DailyIndex {
        public DailyLogEventIndex(IElasticClient client, ILoggerFactory loggerFactory) : base(client, "daily-logevents", 1, loggerFactory) {
            LogEvent = new LogEventType(this);
            AddType(LogEvent);
            AddAlias($"{Name}-today", TimeSpan.FromDays(1));
            AddAlias($"{Name}-last7days", TimeSpan.FromDays(7));
            AddAlias($"{Name}-last30days", TimeSpan.FromDays(30));
        }

        public LogEventType LogEvent { get; }
    }
}