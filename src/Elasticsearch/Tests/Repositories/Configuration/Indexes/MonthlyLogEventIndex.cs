using System;
using Foundatio.Caching;
using Foundatio.Logging;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Tests.Configuration {
    public sealed class MonthlyLogEventIndex : MonthlyIndex {
        public MonthlyLogEventIndex(IElasticClient client, ICacheClient cache = null, ILoggerFactory loggerFactory = null) : base(client, "monthly-logevents", 1, cache, loggerFactory) {
            LogEvent = new LogEventType(this);
            AddType(LogEvent);
            AddAlias($"{Name}-thismonth", TimeSpan.FromDays(32));
            AddAlias($"{Name}-last3months", TimeSpan.FromDays(100));
        }

        public LogEventType LogEvent { get; }
    }
}