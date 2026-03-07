using System;
using System.Linq;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch.QueryDsl;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Options;

namespace Foundatio.Repositories
{
    public enum SearchOperator
    {
        And,
        Or
    }

    public static class QueryExpressionsExtensions
    {
        internal const string SystemFilterKey = "@SystemFilter";

        /// <summary>
        /// System filter that will be applied to the repository query to enforce tenancy or system rules
        /// </summary>
        public static T SystemFilter<T>(this T query, ISystemFilter filter) where T : IRepositoryQuery
        {
            return query.BuildOption(SystemFilterKey, filter);
        }

        internal const string FilterKey = "@FilterExpression";

        /// <summary>
        /// Default Search operator is AND. Does boolean matching and no scoring will occur.
        /// </summary>
        public static T FilterExpression<T>(this T query, string filter) where T : IRepositoryQuery
        {
            return query.BuildOption(FilterKey, filter);
        }

        internal const string SearchKey = "@SearchExpression";
        internal const string CriteriaDefaultOperatorKey = "@SearchExpressionDefaultOperator";

        /// <summary>
        /// Default Search operator is OR and scoring will occur.
        /// </summary>
        public static T SearchExpression<T>(this T query, string search, SearchOperator defaultOperator = SearchOperator.Or) where T : IRepositoryQuery
        {
            query.Values.Set(SearchKey, search);
            return query.BuildOption(CriteriaDefaultOperatorKey, defaultOperator);
        }

        internal const string SortKey = "@SortExpression";

        public static T SortExpression<T>(this T query, string sort) where T : IRepositoryQuery
        {
            return query.BuildOption(SortKey, sort);
        }
    }
}

namespace Foundatio.Repositories.Options
{
    public static class ReadQueryExpressionsExtensions
    {
        public static ISystemFilter GetSystemFilter(this IRepositoryQuery query)
        {
            return query.SafeGetOption<ISystemFilter>(QueryExpressionsExtensions.SystemFilterKey);
        }

        public static string GetFilterExpression(this IRepositoryQuery query)
        {
            return query.SafeGetOption<string>(QueryExpressionsExtensions.FilterKey);
        }

        public static string GetSearchExpression(this IRepositoryQuery query)
        {
            return query.SafeGetOption<string>(QueryExpressionsExtensions.SearchKey);
        }

        public static SearchOperator GetSearchExpressionDefaultOperator(this IRepositoryQuery query)
        {
            return query.SafeGetOption<SearchOperator>(QueryExpressionsExtensions.CriteriaDefaultOperatorKey, SearchOperator.Or);
        }

        public static string GetSortExpression(this IRepositoryQuery query)
        {
            return query.SafeGetOption<string>(QueryExpressionsExtensions.SortKey);
        }
    }
}

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders
{
    public class FieldResolverQueryBuilder : IElasticQueryBuilder
    {
        private readonly QueryFieldResolver _resolver;
        private readonly LuceneQueryParser _parser = new();

        public FieldResolverQueryBuilder(QueryFieldResolver aliasMap)
        {
            _resolver = aliasMap;
        }

        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new()
        {
            string filter = ctx.Source.GetFilterExpression();
            string search = ctx.Source.GetSearchExpression();
            string sort = ctx.Source.GetSortExpression();

            if (!String.IsNullOrEmpty(filter))
            {
                var result = _parser.Parse(filter);
                filter = GenerateQueryVisitor.Run(FieldResolverQueryVisitor.Run(result, _resolver, ctx), ctx);

                ctx.Filter &= new QueryStringQuery
                {
                    Query = filter,
                    DefaultOperator = Operator.And,
                    AnalyzeWildcard = false
                };
            }

            if (!String.IsNullOrEmpty(search))
            {
                var result = _parser.Parse(search);
                search = GenerateQueryVisitor.Run(FieldResolverQueryVisitor.Run(result, _resolver, ctx), ctx);

                ctx.Query &= new QueryStringQuery
                {
                    Query = search,
                    DefaultOperator = ctx.Source.GetSearchExpressionDefaultOperator() == SearchOperator.Or ? Operator.Or : Operator.And,
                    AnalyzeWildcard = true
                };
            }

            if (!String.IsNullOrEmpty(sort))
            {
                var result = _parser.Parse(sort);
                TermToFieldVisitor.Run(result, ctx);
                FieldResolverQueryVisitor.Run(result, _resolver, ctx);

                var sortFields = GetSortFieldsVisitor.Run(result, ctx).ToList();

                // Store sorts in context data - SearchAfterQueryBuilder will apply them
                ctx.Data[SortQueryBuilder.SortFieldsKey] = sortFields;
            }

            return Task.CompletedTask;
        }
    }

    public class ExpressionQueryBuilder : IElasticQueryBuilder
    {
        private readonly LuceneQueryParser _parser = new();

        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new()
        {
            string filter = ctx.Source.GetFilterExpression();
            string search = ctx.Source.GetSearchExpression();
            string sort = ctx.Source.GetSortExpression();

            if (!String.IsNullOrEmpty(filter))
                ctx.Filter &= new QueryStringQuery
                {
                    Query = filter,
                    DefaultOperator = Operator.And,
                    AnalyzeWildcard = false
                };

            if (!String.IsNullOrEmpty(search))
                ctx.Query &= new QueryStringQuery
                {
                    Query = search,
                    DefaultOperator = ctx.Source.GetSearchExpressionDefaultOperator() == SearchOperator.Or ? Operator.Or : Operator.And,
                    AnalyzeWildcard = true
                };

            if (!String.IsNullOrEmpty(sort))
            {
                var result = _parser.Parse(sort);
                TermToFieldVisitor.Run(result, ctx);

                var sortFields = GetSortFieldsVisitor.Run(result, ctx).ToList();

                // Store sorts in context data - SearchAfterQueryBuilder will apply them
                ctx.Data[SortQueryBuilder.SortFieldsKey] = sortFields;
            }

            return Task.CompletedTask;
        }
    }

    public class ParsedExpressionQueryBuilder : IElasticQueryBuilder
    {
        private readonly ElasticQueryParser _parser;

        public ParsedExpressionQueryBuilder(ElasticQueryParser parser)
        {
            _parser = parser ?? throw new ArgumentNullException(nameof(parser));
        }

        public async Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new()
        {
            string filter = ctx.Source.GetFilterExpression();
            string search = ctx.Source.GetSearchExpression();
            string sort = ctx.Source.GetSortExpression();

            // NOTE: Calling UseScoring here to keep the query from being wrapped in a filter which happens ElasticQueryBuilderExtensions.BuildQuery
            if (!String.IsNullOrEmpty(filter))
                ctx.Filter &= await _parser.BuildQueryAsync(filter, ctx.SetDefaultOperator(Operator.And).UseScoring()).AnyContext();

            if (!String.IsNullOrEmpty(search))
                ctx.Query &= await _parser.BuildQueryAsync(search, ctx.UseSearchMode()).AnyContext();

            if (!String.IsNullOrEmpty(sort))
            {
                var sortFields = (await _parser.BuildSortAsync(sort, ctx).AnyContext()).ToList();

                // Store sorts in context data - SearchAfterQueryBuilder will apply them
                ctx.Data[SortQueryBuilder.SortFieldsKey] = sortFields;
            }
        }
    }
}
