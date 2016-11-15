using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public interface ISortableQuery {
        ICollection<IFieldSort> SortFields { get; }
    }

    public class SortableQueryBuilder : IElasticQueryBuilder {
        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var sortableQuery = ctx.GetSourceAs<ISortableQuery>();
            if (sortableQuery?.SortFields == null || sortableQuery.SortFields.Count <= 0)
                return Task.CompletedTask;

            //var opt = ctx.GetOptionsAs<IElasticQueryOptions>();
            //foreach (var sort in sortableQuery.SortFields.Where(s => CanSortByField(opt?.AllowedSortFields, s.Field)))
            //    ctx.Search.Sort(s => s.Field(sort.Field, sort.Order == Foundatio.Repositories.Models.SortOrder.Ascending ? SortOrder.Ascending : SortOrder.Descending));
            ctx.Search.Sort(sortableQuery.SortFields);

            return Task.CompletedTask;
        }

        protected bool CanSortByField(ISet<string> allowedFields, string field) {
            // allow all fields if an allowed list isn't specified
            if (allowedFields == null || allowedFields.Count == 0)
                return true;

            return allowedFields.Contains(field, StringComparer.OrdinalIgnoreCase);
        }
    }
}