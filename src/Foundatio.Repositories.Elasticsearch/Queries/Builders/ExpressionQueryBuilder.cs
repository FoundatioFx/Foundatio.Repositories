using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.QueryDsl;
using Foundatio.Parsers.ElasticQueries;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
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
        public static T FilterExpression<T>(this T query, string? filter) where T : IRepositoryQuery
        {
            if (String.IsNullOrEmpty(filter))
            {
                query.Values.Remove(FilterKey);
                return query;
            }

            return query.BuildOption(FilterKey, filter);
        }

        internal const string SearchKey = "@SearchExpression";
        internal const string CriteriaDefaultOperatorKey = "@SearchExpressionDefaultOperator";

        /// <summary>
        /// Default Search operator is OR and scoring will occur.
        /// </summary>
        public static T SearchExpression<T>(this T query, string? search, SearchOperator defaultOperator = SearchOperator.Or) where T : IRepositoryQuery
        {
            if (String.IsNullOrEmpty(search))
            {
                query.Values.Remove(SearchKey);
                query.Values.Remove(CriteriaDefaultOperatorKey);
                return query;
            }

            query.Values.Set(SearchKey, search);
            return query.BuildOption(CriteriaDefaultOperatorKey, defaultOperator);
        }

        internal const string SortKey = "@SortExpression";

        public static T SortExpression<T>(this T query, string? sort) where T : IRepositoryQuery
        {
            if (String.IsNullOrEmpty(sort))
            {
                query.Values.Remove(SortKey);
                return query;
            }

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
        private readonly ILogger _logger;

        public FieldResolverQueryBuilder(QueryFieldResolver aliasMap, ILogger<FieldResolverQueryBuilder>? logger = null)
        {
            _resolver = aliasMap;
            _logger = logger ?? NullLogger<FieldResolverQueryBuilder>.Instance;
        }

        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new()
        {
            string filter = ctx.Source.GetFilterExpression();
            string search = ctx.Source.GetSearchExpression();
            string sort = ctx.Source.GetSortExpression();

            if (!String.IsNullOrEmpty(filter))
            {
                var result = _parser.Parse(filter);
                var resolved = FieldResolverQueryVisitor.Run(result, _resolver, ctx);
                if (resolved is not null)
                {
                    filter = GenerateQueryVisitor.Run(resolved, ctx);

                    ctx.Filter &= new QueryStringQuery
                    {
                        Query = filter,
                        DefaultOperator = Operator.And,
                        AnalyzeWildcard = false
                    };
                }
                else
                {
                    _logger.LogWarning("No resolved query root; filter omitted for: {Expression}", filter);
                }
            }

            if (!String.IsNullOrEmpty(search))
            {
                var result = _parser.Parse(search);
                var resolved = FieldResolverQueryVisitor.Run(result, _resolver, ctx);
                if (resolved is not null)
                {
                    search = GenerateQueryVisitor.Run(resolved, ctx);

                    ctx.Query &= new QueryStringQuery
                    {
                        Query = search,
                        DefaultOperator = ctx.Source.GetSearchExpressionDefaultOperator() == SearchOperator.Or ? Operator.Or : Operator.And,
                        AnalyzeWildcard = true
                    };
                }
                else
                {
                    _logger.LogWarning("No resolved query root; search omitted for: {Expression}", search);
                }
            }

            if (!String.IsNullOrEmpty(sort))
            {
                var result = _parser.Parse(sort);
                TermToFieldVisitor.Run(result, ctx);
                var resolved = FieldResolverQueryVisitor.Run(result, _resolver, ctx);
                if (resolved is null)
                {
                    _logger.LogWarning("No resolved query root; sort omitted for: {Expression}", sort);
                }
                else
                {
                    var sortFields = GetSortFieldsVisitor.Run(resolved, ctx).ToList();
                    ctx.Data[SortQueryBuilder.SortFieldsKey] = sortFields;
                }
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

                if (ctx.Data.TryGetValue(SortQueryBuilder.SortFieldsKey, out var existingSorts) && existingSorts is List<SortOptions> existing)
                    existing.AddRange(sortFields);
                else
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

                if (ctx.Data.TryGetValue(SortQueryBuilder.SortFieldsKey, out var existingSorts) && existingSorts is List<SortOptions> existing)
                    existing.AddRange(sortFields);
                else
                    ctx.Data[SortQueryBuilder.SortFieldsKey] = sortFields;
            }
        }
    }
}
