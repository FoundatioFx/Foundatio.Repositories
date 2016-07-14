using System;
using System.Linq;
using Foundatio.Repositories.Elasticsearch.Queries.Options;
using Foundatio.Repositories.Queries;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public class SortableQueryBuilder : IElasticQueryBuilder {
        public void Build<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var sortableQuery = ctx.GetQueryAs<ISortableQuery>();
            if (sortableQuery?.SortBy == null || sortableQuery.SortBy.Count <= 0)
                return;

            var opt = ctx.GetOptionsAs<IQueryOptions>();
            foreach (var sort in sortableQuery.SortBy.Where(s => CanSortByField(opt?.AllowedSortFields, s.Field)))
                ctx.Search.Sort(s => s.OnField(sort.Field)
                    .Order(sort.Order == Foundatio.Repositories.Models.SortOrder.Ascending ? SortOrder.Ascending : SortOrder.Descending));
        }

        protected bool CanSortByField(string[] allowedFields, string field) {
            // allow all fields if an allowed list isn't specified
            if (allowedFields == null || allowedFields.Length == 0)
                return true;

            return allowedFields.Contains(field, StringComparer.OrdinalIgnoreCase);
        }
    }
}