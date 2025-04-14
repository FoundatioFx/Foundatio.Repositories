using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Options;

namespace Foundatio.Repositories
{
    public static class SortQueryExtensions
    {
        internal const string SortsKey = "@SortsKey";

        public static T Sort<T>(this T query, Field field, SortOrder? order = null) where T : IRepositoryQuery
        {
            return query.AddCollectionOptionValue<T, SortOptions>(SortsKey, new FieldSort { Field = field, Order = order });
        }

        public static T SortDescending<T>(this T query, Field field) where T : IRepositoryQuery
        {
            return query.Sort(field, SortOrder.Desc);
        }

        public static T SortAscending<T>(this T query, Field field) where T : IRepositoryQuery
        {
            return query.Sort(field, SortOrder.Asc);
        }

        public static IRepositoryQuery<T> Sort<T>(this IRepositoryQuery<T> query, Expression<Func<T, object>> objectPath, SortOrder? order = null) where T : class
        {
            return query.AddCollectionOptionValue<IRepositoryQuery<T>, SortOptions>(SortsKey, new FieldSort { Field = objectPath, Order = order });
        }

        public static IRepositoryQuery<T> SortDescending<T>(this IRepositoryQuery<T> query, Expression<Func<T, object>> objectPath) where T : class
        {
            return query.Sort(objectPath, SortOrder.Desc);
        }

        public static IRepositoryQuery<T> SortAscending<T>(this IRepositoryQuery<T> query, Expression<Func<T, object>> objectPath) where T : class
        {
            return query.Sort(objectPath, SortOrder.Asc);
        }
    }
}

namespace Foundatio.Repositories.Options
{
    public static class ReadSortQueryExtensions
    {
        public static ICollection<SortOptions> GetSorts(this IRepositoryQuery query)
        {
            return query.SafeGetCollection<SortOptions>(SortQueryExtensions.SortsKey);
        }
    }
}

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders
{
    public class SortQueryBuilder : IElasticQueryBuilder
    {
        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new()
        {
            var sortFields = ctx.Source.GetSorts();
            if (sortFields.Count <= 0)
                return Task.CompletedTask;

            var resolver = ctx.GetMappingResolver();
            sortFields = resolver.GetResolvedFields(sortFields);

            ctx.Search.Sort(sortFields);
            return Task.CompletedTask;
        }
    }
}
