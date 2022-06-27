using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Exceptionless.DateTimeExtensions;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Options;
using Foundatio.Utility;
using Nest;

namespace Foundatio.Repositories {
    [DebuggerDisplay("{Field}: {StartDate} - {EndDate}")]
    public class DateRange {
        public DateTime? StartDate { get; set; }
        public DateTime? EndDate { get; set; }
        public string TimeZone { get; set; }
        public Field Field { get; set; }

        public bool UseStartDate => StartDate.HasValue && StartDate.Value > DateTime.MinValue;

        public bool UseEndDate => EndDate.HasValue && EndDate.Value < DateTime.MaxValue;

        public bool UseDateRange => Field != null && (UseStartDate || UseEndDate);

        public DateTime GetStartDate() {
            return UseStartDate ? StartDate.GetValueOrDefault() : DateTime.MinValue;
        }

        public DateTime GetEndDate() {
            return UseEndDate ? EndDate.GetValueOrDefault() : SystemClock.UtcNow.AddHours(1);
        }
    }

    public static class DateRangesQueryExtensions {
        internal const string DateRangesKey = "@DateRanges";

        public static T DateRange<T>(this T query, DateTime? utcStart, DateTime? utcEnd, Field field, string timeZone = null) where T : IRepositoryQuery {
            if (field == null)
                throw new ArgumentNullException(nameof(field));

            if (!utcStart.HasValue && !utcEnd.HasValue)
                throw new ArgumentNullException(nameof(utcStart), "Start date and end date cannot be null.");
            
            if (utcStart.HasValue && utcEnd.HasValue && utcStart.Value.IsAfter(utcEnd.Value))
                throw new ArgumentOutOfRangeException(nameof(utcStart), "Start date must be before end date.");

            return query.AddCollectionOptionValue(DateRangesKey, new DateRange {
                StartDate = utcStart,
                EndDate = utcEnd,
                Field = field,
                TimeZone = timeZone
            });
        }

        public static T DateRange<T, TModel>(this T query, DateTime? utcStart, DateTime? utcEnd, Expression<Func<TModel, object>> objectPath, string timeZone = null) where T : IRepositoryQuery {
            if (objectPath == null)
                throw new ArgumentNullException(nameof(objectPath));

            if (!utcStart.HasValue && !utcEnd.HasValue)
                throw new ArgumentNullException(nameof(utcStart), "Start date and end date cannot be null.");
            
            if (utcStart.HasValue && utcEnd.HasValue && utcStart.Value.IsAfter(utcEnd.Value))
                throw new ArgumentOutOfRangeException(nameof(utcStart), "Start date must be before end date.");

            return query.AddCollectionOptionValue(DateRangesKey, new DateRange {
                StartDate = utcStart,
                EndDate = utcEnd,
                Field = objectPath,
                TimeZone = timeZone
            });
        }
    }
}

namespace Foundatio.Repositories.Options {
    public static class ReadDateRangesQueryExtensions {
        public static ICollection<DateRange> GetDateRanges(this IRepositoryQuery query) {
            return query.SafeGetCollection<DateRange>(DateRangesQueryExtensions.DateRangesKey);
        }
    }
}

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public class DateRangeQueryBuilder : IElasticQueryBuilder {
        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class {
            var dateRanges = ctx.Source.GetDateRanges();
            if (dateRanges.Count <= 0)
                return Task.CompletedTask;

            var resolver = ctx.GetMappingResolver();

            foreach (var dateRange in dateRanges.Where(dr => dr.UseDateRange)) {
                var rangeQuery = new DateRangeQuery { Field = resolver.ResolveFieldName(dateRange.Field) };
                if (dateRange.UseStartDate)
                    rangeQuery.GreaterThanOrEqualTo = dateRange.GetStartDate();
                if (dateRange.UseEndDate)
                    rangeQuery.LessThanOrEqualTo = dateRange.GetEndDate();
                if (!String.IsNullOrEmpty(dateRange.TimeZone))
                    rangeQuery.TimeZone = dateRange.TimeZone;
                
                ctx.Filter &= rangeQuery;
            }

            return Task.CompletedTask;
        }
    }
}