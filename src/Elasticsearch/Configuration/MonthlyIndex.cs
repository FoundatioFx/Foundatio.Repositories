using System;
using System.Collections.Generic;
using Exceptionless.DateTimeExtensions;
using Foundatio.Caching;
using Foundatio.Logging;
using Foundatio.Utility;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Configuration {
    public class MonthlyIndexType<T> : TimeSeriesIndexType<T> where T : class {
        public MonthlyIndexType(IIndex index, string name = null, Func<T, DateTime> getDocumentDateUtc = null) 
            : base(index, name, getDocumentDateUtc) { }
    }

    public class MonthlyIndex: DailyIndex {
        public MonthlyIndex(IElasticClient client, string name, int version = 1, ICacheClient cache = null, ILoggerFactory loggerFactory = null) 
            : base(client, name, version, cache, loggerFactory) {
            DateFormat = "yyyy.MM";
        }
        
        public override string[] GetIndexes(DateTime? utcStart, DateTime? utcEnd) {
            if (!utcStart.HasValue)
                utcStart = SystemClock.UtcNow;

            if (!utcEnd.HasValue || utcEnd.Value < utcStart)
                utcEnd = SystemClock.UtcNow;

            var utcEndOfMonth = utcEnd.Value.EndOfMonth();

            var indices = new List<string>();
            for (DateTime current = utcStart.Value.StartOfMonth(); current <= utcEndOfMonth; current = current.AddMonths(1))
                indices.Add(GetIndex(current));

            return indices.ToArray();
        }

        protected override DateTime GetIndexExpirationDate(DateTime utcDate) {
            return MaxIndexAge.HasValue && MaxIndexAge > TimeSpan.Zero ? utcDate.StartOfMonth().Add(MaxIndexAge.Value) : DateTime.MaxValue;
        }

        protected override bool ShouldCreateAlias(DateTime documentDateUtc, IndexAliasAge alias) {
            if (SystemClock.UtcNow.StartOfMonth() == documentDateUtc.StartOfMonth())
                return true;

            return base.ShouldCreateAlias(documentDateUtc, alias);
        }
    }
}