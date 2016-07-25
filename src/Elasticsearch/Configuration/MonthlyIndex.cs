using System;
using System.Collections.Generic;
using System.Globalization;
using Exceptionless.DateTimeExtensions;
using Foundatio.Logging;
using Foundatio.Utility;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Configuration {
    public class MonthlyIndexType<T> : TimeSeriesIndexType<T> where T : class {
        public MonthlyIndexType(IIndex index, string name = null, Func<T, DateTime> getDocumentDateUtc = null) : base(index, name, getDocumentDateUtc) { }
    }

    public class MonthlyIndex: DailyIndex {
        public MonthlyIndex(IElasticClient client, string name, int version = 1, ILoggerFactory loggerFactory = null): base(client, name, version, loggerFactory) {}

        public override string GetIndex(DateTime utcDate) {
            return $"{VersionedNamePrefix}-{utcDate:yyyy.MM}";
        }

        public override string[] GetIndexes(DateTime? utcStart, DateTime? utcEnd) {
            if (!utcStart.HasValue)
                utcStart = SystemClock.UtcNow;

            if (!utcEnd.HasValue || utcEnd.Value < utcStart)
                utcEnd = SystemClock.UtcNow;

            var utcEndOfDay = utcEnd.Value.EndOfDay();

            var indices = new List<string>();
            for (DateTime current = utcStart.Value; current <= utcEndOfDay; current = current.AddMonths(1))
                indices.Add(GetIndex(current));

            return indices.ToArray();
        }

        protected override DateTime GetIndexDate(string name) {
            DateTime result;
            if (DateTime.TryParseExact(name, "'" + VersionedNamePrefix + "-'yyyy.MM", EnUs, DateTimeStyles.None, out result))
                return result;

            return DateTime.MaxValue;
        }
    }
}