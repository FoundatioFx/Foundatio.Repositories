using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch.QueryDsl;
using Foundatio.Repositories.Options;

namespace Foundatio.Repositories
{
    public static class ChildQueryExtensions
    {
        internal const string ChildQueriesKey = "@ChildQueries";

        public static TQuery ChildQuery<TQuery>(this TQuery query, IRepositoryQuery childQuery) where TQuery : IRepositoryQuery
        {
            if (childQuery == null)
                throw new ArgumentNullException(nameof(childQuery));

            if (childQuery.GetDocumentType() == typeof(object))
                throw new ArgumentException("DocumentType must be set on child queries", nameof(childQuery));

            return query.AddCollectionOptionValue(ChildQueriesKey, childQuery);
        }

        public static TQuery ChildQuery<TQuery>(this TQuery query, RepositoryQueryDescriptor childQuery) where TQuery : IRepositoryQuery
        {
            if (childQuery == null)
                throw new ArgumentNullException(nameof(childQuery));

            var q = childQuery.Configure();
            if (q.GetDocumentType() == typeof(object))
                throw new ArgumentException("DocumentType must be set on child queries", nameof(childQuery));

            return query.AddCollectionOptionValue(ChildQueriesKey, childQuery.Configure());
        }
    }
}

namespace Foundatio.Repositories.Options
{
    public static class ReadChildQueryExtensions
    {
        public static ICollection<IRepositoryQuery> GetChildQueries(this IRepositoryQuery query)
        {
            return query.SafeGetCollection<IRepositoryQuery>(ChildQueryExtensions.ChildQueriesKey);
        }
    }
}

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders
{
    public class ChildQueryBuilder : IElasticQueryBuilder
    {
        public async Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new()
        {
            var childQueries = ctx.Source.GetChildQueries();
            if (childQueries.Count == 0)
                return;

            var index = ctx.Options.GetElasticIndex();
            foreach (var childQuery in childQueries)
            {
                var childOptions = ctx.Options.Clone();
                childOptions.DocumentType(childQuery.GetDocumentType());
                var childContext = new QueryBuilderContext<object>(childQuery, childOptions, null);

                await index.QueryBuilder.BuildAsync(childContext);

                if (childContext.Filter != null)
                    ctx.Filter &= new HasChildQuery
                    {
                        Type = childQuery.GetDocumentType().Name.ToLowerInvariant(),
                        Query = new BoolQuery
                        {
                            Filter = new[] { childContext.Filter }
                        }
                    };

                if (childContext.Query != null)
                    ctx.Query &= new HasChildQuery
                    {
                        Type = childQuery.GetDocumentType().Name.ToLowerInvariant(),
                        Query = new BoolQuery
                        {
                            Must = new[] { childContext.Query }
                        }
                    };
            }
        }
    }
}
