using System;
using System.Collections.Generic;
using Exceptionless.DateTimeExtensions;
using Foundatio.Utility;

namespace Foundatio.Repositories.Elasticsearch.Configuration {
    public class MonthlyIndexType<T> : TimeSeriesIndexType<T> where T : class {
        public MonthlyIndexType(IIndex index, string name = null, Func<T, DateTime> getDocumentDateUtc = null)
            : base(index, name, getDocumentDateUtc) { }
    }

    public class MonthlyIndex: DailyIndex {
        public MonthlyIndex(IElasticConfiguration configuration, string name, int version = 1)
            : base(configuration, name, version) {
            DateFormat = "yyyy.MM";
        }

        public override string[] GetIndexes(DateTime? utcStart, DateTime? utcEnd) {
            if (!utcStart.HasValue)
                utcStart = SystemClock.UtcNow;

            if (!utcEnd.HasValue || utcEnd.Value < utcStart)
                utcEnd = SystemClock.UtcNow;

            var utcStartOfDay = utcStart.Value.StartOfDay();
            var utcEndOfDay = utcEnd.Value.EndOfDay();
            var period = utcEndOfDay - utcStartOfDay;
            if ((MaxIndexAge.HasValue && period > MaxIndexAge.Value) || period.GetTotalYears() > 1)
                return new string[0];

            var utcEndOfMonth = utcEnd.Value.EndOfMonth();

            var indices = new List<string>();
            for (var current = utcStartOfDay; current <= utcEndOfMonth; current = current.AddMonths(1))
                indices.Add(GetIndex(current));

            return indices.ToArray();
        }

        protected override DateTime GetIndexExpirationDate(DateTime utcDate) {
            return MaxIndexAge.HasValue && MaxIndexAge > TimeSpan.Zero ? utcDate.EndOfMonth().SafeAdd(MaxIndexAge.Value) : DateTime.MaxValue;
        }

        protected override bool ShouldCreateAlias(DateTime documentDateUtc, IndexAliasAge alias) {
            if (alias.MaxAge == TimeSpan.MaxValue)
                return true;

            return SystemClock.UtcNow.Date.SafeSubtract(alias.MaxAge) <= documentDateUtc.EndOfMonth();
        }
    }
}