using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Foundatio.Repositories.Options;
using Foundatio.Utility;
using Nest;

namespace Foundatio.Repositories {
    [DebuggerDisplay("{Field}: {StartDate} - {EndDate}")]
    public class DateRange {
        public DateTime? StartDate { get; set; }
        public DateTime? EndDate { get; set; }
        public Field Field { get; set; }

        public bool UseStartDate => StartDate.HasValue && StartDate.Value > DateTime.MinValue;

        public bool UseEndDate => EndDate.HasValue;

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

        public static T DateRange<T>(this T query, DateTime? utcStart, DateTime? utcEnd, Field field) where T : IRepositoryQuery {
            if (field == null)
                throw new ArgumentNullException(nameof(field));

            return query.AddCollectionOptionValue(DateRangesKey, new DateRange { StartDate = utcStart, EndDate = utcEnd, Field = field });
        }

        public static T DateRange<T, TModel>(this T query, DateTime? utcStart, DateTime? utcEnd, Expression<Func<TModel, object>> objectPath) where T : IRepositoryQuery {
            if (objectPath == null)
                throw new ArgumentNullException(nameof(objectPath));

            return query.AddCollectionOptionValue(DateRangesKey, new DateRange { StartDate = utcStart, EndDate = utcEnd, Field = objectPath });
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
        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var dateRanges = ctx.Source.GetDateRanges();
            if (dateRanges.Count <= 0)
                return Task.CompletedTask;

            var elasticQueryOptions = ctx.Options.GetElasticTypeSettings();
            foreach (var dateRange in dateRanges.Where(dr => dr.UseDateRange)) {
                string fieldName = dateRange.Field?.Name;
                if (elasticQueryOptions?.IndexType != null && !String.IsNullOrEmpty(fieldName))
                    fieldName = elasticQueryOptions.IndexType.GetFieldName(fieldName);

                ctx.Filter &= new DateRangeQuery {
                    Field = fieldName ?? dateRange.Field,
                    GreaterThanOrEqualTo = dateRange.GetStartDate(),
                    LessThanOrEqualTo = dateRange.GetEndDate()
                };
            }

            return Task.CompletedTask;
        }
    }
}