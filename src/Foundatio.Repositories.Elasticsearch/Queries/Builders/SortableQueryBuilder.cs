using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Repositories.Elasticsearch.Queries.Options;
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

            var opt = ctx.GetOptionsAs<IElasticQueryOptions>();
            // TODO: Check SortFields against opt.AllowedSortFields
            //foreach (var sort in sortableQuery.SortFields.Where(s => CanSortByField(opt?.AllowedSortFields, s.Field.ToString())))
            //    ctx.Search.Sort(s => s.OnField(sort.Field)
            //        .Order(sort.Order == Foundatio.Repositories.Models.SortOrder.Ascending ? SortOrder.Ascending : SortOrder.Descending));

            ctx.Search.Sort(sortableQuery.SortFields);

            return Task.CompletedTask;
        }
    }
}