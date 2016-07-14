using System;
using System.Collections.Generic;
using System.Linq;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public interface IDateRangeQuery {
        List<DateRange> DateRanges { get; }
    }

    public class DateRange {
        public DateTime? StartDate { get; set; }
        public DateTime? EndDate { get; set; }
        public string Field { get; set; }

        public bool UseStartDate => StartDate.HasValue && StartDate.Value > DateTime.MinValue;

        public bool UseEndDate => EndDate.HasValue && EndDate.Value < DateTime.UtcNow.AddHours(1);

        public bool UseDateRange => Field != null && (UseStartDate || UseEndDate);

        public DateTime GetStartDate() {
            return UseStartDate ? StartDate.GetValueOrDefault() : DateTime.MinValue;
        }

        public DateTime GetEndDate() {
            return UseEndDate ? EndDate.GetValueOrDefault() : DateTime.UtcNow.AddHours(1);
        }
    }

    public class DateRangeQueryBuilder : IElasticQueryBuilder {
        public void Build<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var dateRangeQuery = ctx.GetQueryAs<IDateRangeQuery>();
            if (dateRangeQuery?.DateRanges == null || dateRangeQuery.DateRanges.Count <= 0)
                return;

            foreach (var dateRange in dateRangeQuery.DateRanges.Where(dr => dr.UseDateRange)) {
                ctx.Filter &= new RangeFilter {
                    Field = dateRange.Field,
                    GreaterThanOrEqualTo = dateRange.GetStartDate().ToString("o"),
                    LowerThanOrEqualTo = dateRange.GetEndDate().ToString("O")
                };
            }
        }
    }

    public static class DateRangeQueryExtensions {
        public static T WithDateRange<T>(this T query, DateTime? start, DateTime? end, string field) where T : IDateRangeQuery {
            query.DateRanges?.Add(new DateRange { StartDate = start, EndDate = end, Field = field });
            return query;
        }
    }
}