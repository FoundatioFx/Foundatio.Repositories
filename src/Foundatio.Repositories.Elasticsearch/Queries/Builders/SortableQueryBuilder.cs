using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Queries;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public interface ISortableQuery : IRepositoryQuery {
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

    public static class SortableQueryExtensions {
        public static T WithSort<T>(this T query, Field field, SortOrder? order = null) where T: ISortableQuery {
            query.SortFields?.Add(new SortField { Field = field, Order = order });
            return query;
        }

        public static T WithSortDescending<T>(this T query, Field field) where T : ISortableQuery {
            return query.WithSort(field, SortOrder.Descending);
        }

        public static T WithSortAscending<T>(this T query, Field field) where T : ISortableQuery {
            return query.WithSort(field, SortOrder.Ascending);
        }

        public static T WithSort<T, TModel>(this T query, Expression<Func<TModel, object>> objectPath, SortOrder? order = null) where T : ISortableQuery {
            query.SortFields?.Add(new SortField { Field = objectPath, Order = order });
            return query;
        }

        public static T WithSortDescending<T, TModel>(this T query, Expression<Func<TModel, object>> objectPath) where T : ISortableQuery {
            return query.WithSort(objectPath, SortOrder.Descending);
        }

        public static T WithSortAscending<T, TModel>(this T query, Expression<Func<TModel, object>> objectPath) where T : ISortableQuery {
            return query.WithSort(objectPath, SortOrder.Ascending);
        }
    }
}