using System;
using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Options;
using Nest;

namespace Foundatio.Repositories {
    public enum SearchOperator {
        And,
        Or
    }

    public static class QueryExpressionsExtensions {
        internal const string FilterKey = "@FilterExpression";

        /// <summary>
        /// Default Search operator is AND. Does boolean matching and no scoring will occur.
        /// </summary>
        public static T FilterExpression<T>(this T query, string filter) where T : IRepositoryQuery {
            return query.BuildOption(FilterKey, filter);
        }

        internal const string SearchKey = "@SearchExpression";
        internal const string CriteriaDefaultOperatorKey = "@SearchExpressionDefaultOperator";

        /// <summary>
        /// Default Search operator is OR and scoring will occur.
        /// </summary>
        public static T SearchExpression<T>(this T query, string search, SearchOperator defaultOperator = SearchOperator.Or) where T : IRepositoryQuery {
            query.Values.Set(SearchKey, search);
            return query.BuildOption(CriteriaDefaultOperatorKey, defaultOperator);
        }

        internal const string SortKey = "@SortExpression";

        public static T SortExpression<T>(this T query, string sort) where T : IRepositoryQuery {
            return query.BuildOption(SortKey, sort);
        }
    }
}

namespace Foundatio.Repositories.Options {
    public static class ReadQueryExpressionsExtensions {
        public static string GetFilterExpression(this IRepositoryQuery query) {
            return query.SafeGetOption<string>(QueryExpressionsExtensions.FilterKey);
        }

        public static string GetSearchExpression(this IRepositoryQuery query) {
            return query.SafeGetOption<string>(QueryExpressionsExtensions.SearchKey);
        }

        public static SearchOperator GetSearchExpressionDefaultOperator(this IRepositoryQuery query) {
            return query.SafeGetOption<SearchOperator>(QueryExpressionsExtensions.CriteriaDefaultOperatorKey, SearchOperator.Or);
        }

        public static string GetSortExpression(this IRepositoryQuery query) {
            return query.SafeGetOption<string>(QueryExpressionsExtensions.SortKey);
        }
    }
}

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public class AliasedExpressionQueryBuilder : IElasticQueryBuilder {
        private readonly AliasMap _aliasMap;
        private readonly LuceneQueryParser _parser = new LuceneQueryParser();

        public AliasedExpressionQueryBuilder(AliasMap aliasMap) {
            _aliasMap = aliasMap;
        }

        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            string filter = ctx.Source.GetFilterExpression();
            string search = ctx.Source.GetSearchExpression();
            string sort = ctx.Source.GetSortExpression();

            if (!String.IsNullOrEmpty(filter)) {
                var result = _parser.Parse(filter);
                filter = GenerateQueryVisitor.Run(AliasedQueryVisitor.Run(result, _aliasMap, ctx), ctx);

                ctx.Filter &= new QueryStringQuery {
                    Query = filter,
                    DefaultOperator = Operator.And,
                    AnalyzeWildcard = false
                };
            }

            if (!String.IsNullOrEmpty(search)) {
                var result = _parser.Parse(search);
                search = GenerateQueryVisitor.Run(AliasedQueryVisitor.Run(result, _aliasMap, ctx), ctx);

                ctx.Query &= new QueryStringQuery {
                    Query = search,
                    DefaultOperator = ctx.Source.GetSearchExpressionDefaultOperator() == SearchOperator.Or ? Operator.Or : Operator.And,
                    AnalyzeWildcard = true
                };
            }

            if (!String.IsNullOrEmpty(sort)) {
                var result = _parser.Parse(sort);
                var opt = ctx.Options.GetElasticTypeSettings();
                TermToFieldVisitor.Run(result, ctx);
                AliasedQueryVisitor.Run(result, _aliasMap, ctx);
                var fields = GetReferencedFieldsQueryVisitor.Run(result);
                // TODO: Check referenced fields against opt.AllowedSortFields

                var fieldSort = GetSortFieldsVisitor.Run(result, ctx);
                ctx.Search.Sort(fieldSort);
            }

            return Task.CompletedTask;
        }
    }

    public class ExpressionQueryBuilder : IElasticQueryBuilder {
        private readonly LuceneQueryParser _parser = new LuceneQueryParser();

        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            string filter = ctx.Source.GetFilterExpression();
            string search = ctx.Source.GetSearchExpression();
            string sort = ctx.Source.GetSortExpression();

            if (!String.IsNullOrEmpty(filter))
                ctx.Filter &= new QueryStringQuery {
                    Query = filter,
                    DefaultOperator = Operator.And,
                    AnalyzeWildcard = false
                };

            if (!String.IsNullOrEmpty(search))
                ctx.Query &= new QueryStringQuery {
                    Query = search,
                    DefaultOperator = ctx.Source.GetSearchExpressionDefaultOperator() == SearchOperator.Or ? Operator.Or : Operator.And,
                    AnalyzeWildcard = true
                };

            if (!String.IsNullOrEmpty(sort)) {
                var result = _parser.Parse(sort);
                var opt = ctx.Options.GetElasticTypeSettings();
                TermToFieldVisitor.Run(result, ctx);
                var fields = GetReferencedFieldsQueryVisitor.Run(result);
                // TODO: Check referenced fields against opt.AllowedSortFields

                var sortField = GetSortFieldsVisitor.Run(result, ctx);
                ctx.Search.Sort(sortField);
            }

            return Task.CompletedTask;
        }
    }

    public class ParsedExpressionQueryBuilder : IElasticQueryBuilder {
        private readonly ElasticQueryParser _parser;

        public ParsedExpressionQueryBuilder(ElasticQueryParser parser) {
            _parser = parser ?? throw new ArgumentNullException(nameof(parser));
        }

        public async Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            string filter = ctx.Source.GetFilterExpression();
            string search = ctx.Source.GetSearchExpression();
            string sort = ctx.Source.GetSortExpression();

            // NOTE: Calling UseScoring here to keep the query from being wrapped in a filter which happens ElasticQueryBuilderExtensions.BuildQuery
            if (!String.IsNullOrEmpty(filter))
                ctx.Filter &= await _parser.BuildQueryAsync(filter, ctx.SetDefaultOperator(Operator.And).UseScoring()).AnyContext();

            if (!String.IsNullOrEmpty(search))
                ctx.Query &= await _parser.BuildQueryAsync(search, ctx.SetDefaultOperator(Operator.Or).UseScoring()).AnyContext();

            if (!String.IsNullOrEmpty(sort))
                ctx.Search.Sort(await _parser.BuildSortAsync(sort, ctx).AnyContext());
        }
    }
}