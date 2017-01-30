using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Foundatio.Repositories.Queries;
using Foundatio.Utility;
using Nest;
using Foundatio.Repositories.Elasticsearch.Options;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public interface IDateRangeQuery : IRepositoryQuery {
        ICollection<DateRange> DateRanges { get; }
    }

    [DebuggerDisplay("{Field}: {StartDate} - {EndDate}")]
    public class DateRange {
        public DateTime? StartDate { get; set; }
        public DateTime? EndDate { get; set; }
        public Field Field { get; set; }

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

            var elasticQueryOptions = ctx.GetOptionsAs<IElasticCommandOptions>();
            foreach (var dateRange in dateRangeQuery.DateRanges.Where(dr => dr.UseDateRange)) {
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

    public static class DateRangeQueryExtensions {
        public static T WithDateRange<T>(this T query, DateTime? utcStart, DateTime? utcEnd, Field field) where T : IDateRangeQuery {
            if (field == null)
                throw new ArgumentNullException(nameof(field));

            query.DateRanges?.Add(new DateRange { StartDate = utcStart, EndDate = utcEnd, Field = field });
            return query;
        }

        public static T WithDateRange<T, TModel>(this T query, DateTime? utcStart, DateTime? utcEnd, Expression<Func<TModel, object>> objectPath) where T : IDateRangeQuery {
            if (objectPath == null)
                throw new ArgumentNullException(nameof(objectPath));

            query.DateRanges?.Add(new DateRange { StartDate = utcStart, EndDate = utcEnd, Field = objectPath });
            return query;
        }
    }
}