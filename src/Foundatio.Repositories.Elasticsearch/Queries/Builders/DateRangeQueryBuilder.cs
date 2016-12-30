using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Foundatio.Utility;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public interface IDateRangeQuery {
        ICollection<DateRange> DateRanges { get; }
    }

    [DebuggerDisplay("{Field}: {StartDate} - {EndDate}")]
    public class DateRange {
        public DateTime? StartDate { get; set; }
        public DateTime? EndDate { get; set; }
        public string Field { get; set; }

        public bool UseStartDate => StartDate.HasValue && StartDate.Value > DateTime.MinValue;

        public bool UseEndDate => EndDate.HasValue && EndDate.Value < SystemClock.UtcNow.AddHours(1);

        public bool UseDateRange => Field != null && (UseStartDate || UseEndDate);

        public DateTime GetStartDate() {
            return UseStartDate ? StartDate.GetValueOrDefault() : DateTime.MinValue;
        }

        public DateTime GetEndDate() {
            return UseEndDate ? EndDate.GetValueOrDefault() : SystemClock.UtcNow.AddHours(1);
        }
    }

    public class DateRangeQueryBuilder : IElasticQueryBuilder {
        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var dateRangeQuery = ctx.GetSourceAs<IDateRangeQuery>();
            if (dateRangeQuery?.DateRanges == null || dateRangeQuery.DateRanges.Count <= 0)
                return Task.CompletedTask;

            var resolver = (ctx as IQueryVisitorContextWithAliasResolver)?.RootAliasResolver;
            foreach (var dateRange in dateRangeQuery.DateRanges.Where(dr => dr.UseDateRange)) {
                string field = dateRange.Field;
                if (resolver != null)
                    field = resolver(dateRange.Field)?.Name ?? dateRange.Field;

                ctx.Filter &= new DateRangeQuery {
                    Field = field,
                    GreaterThanOrEqualTo = dateRange.GetStartDate(),
                    LessThanOrEqualTo = dateRange.GetEndDate()
                };
            }

            return Task.CompletedTask;
        }
    }

    public static class DateRangeQueryExtensions {
        public static T WithDateRange<T>(this T query, DateTime? utcStart, DateTime? utcEnd, string field) where T : IDateRangeQuery {
            if (String.IsNullOrEmpty(field))
                throw new ArgumentNullException(nameof(field));

            query.DateRanges?.Add(new DateRange { StartDate = utcStart, EndDate = utcEnd, Field = field });
            return query;
        }
    }
}