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
            return $"{VersionedName}-{utcDate:yyyy.MM}";
        }

        public override string GetVersionedIndex(DateTime utcDate, int? version = null) {
            if (version == null || version < 0)
                version = Version;

            return $"{Name}-v{Version}-{utcDate:yyyy.MM}";
        }

        protected override DateTime GetIndexDate(string name) {
            DateTime result;
            if (DateTime.TryParseExact(name, $"\'{VersionedName}-\'yyyy.MM", EnUs, DateTimeStyles.AssumeUniversal, out result))
                return result.Date;

            return DateTime.MaxValue;
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
    }
}