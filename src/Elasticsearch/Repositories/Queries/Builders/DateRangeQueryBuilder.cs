using System;
using System.Linq;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
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
}