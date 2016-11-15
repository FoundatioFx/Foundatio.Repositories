using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Foundatio.Repositories.Elasticsearch.Queries.Options;
using Foundatio.Repositories.Extensions;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public interface ISearchQuery {
        string Filter { get; set; }
        string Criteria { get; set; }
        string Sort { get; set; }
        SearchOperator DefaultCriteriaOperator { get; set; }
    }

    public enum SearchOperator {
        And,
        Or
    }

    public class AliasedSearchQueryBuilder : IElasticQueryBuilder {
        private readonly AliasMap _aliasMap;
        private readonly LuceneQueryParser _parser = new LuceneQueryParser();

        public AliasedSearchQueryBuilder(AliasMap aliasMap) {
            _aliasMap = aliasMap;
        }

        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var searchQuery = ctx.GetSourceAs<ISearchQuery>();
            if (searchQuery == null)
                return Task.CompletedTask;

            if (!String.IsNullOrEmpty(searchQuery.Filter)) {
                var result = _parser.Parse(searchQuery.Filter);
                searchQuery.Filter = GenerateQueryVisitor.Run(AliasedQueryVisitor.Run(result, _aliasMap, ctx), ctx);

                ctx.Filter &= new QueryStringQuery {
                    Query = searchQuery.Filter,
                    DefaultOperator = Operator.And,
                    AnalyzeWildcard = false
                };
            }

            if (!String.IsNullOrEmpty(searchQuery.Criteria)) {
                var result = _parser.Parse(searchQuery.Criteria);
                searchQuery.Criteria = GenerateQueryVisitor.Run(AliasedQueryVisitor.Run(result, _aliasMap, ctx), ctx);

                ctx.Query &= new QueryStringQuery {
                    Query = searchQuery.Criteria,
                    DefaultOperator = searchQuery.DefaultCriteriaOperator == SearchOperator.Or ? Operator.Or : Operator.And,
                    AnalyzeWildcard = true
                };
            }

            if (!String.IsNullOrEmpty(searchQuery.Sort)) {
                var result = _parser.Parse(searchQuery.Sort);
                var opt = ctx.GetOptionsAs<IElasticQueryOptions>();
                TermToFieldVisitor.Run(result, ctx);
                AliasedQueryVisitor.Run(result, _aliasMap, ctx);
                var fields = GetReferencedFieldsQueryVisitor.Run(result);
                // TODO: Check referenced fields against opt.AllowedSortFields

                var sort = GetSortFieldsVisitor.Run(result, ctx);
                ctx.Search.Sort(sort);
            }

            return Task.CompletedTask;
        }
    }

    public class SearchQueryBuilder : IElasticQueryBuilder {
        private readonly LuceneQueryParser _parser = new LuceneQueryParser();

        public Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var searchQuery = ctx.GetSourceAs<ISearchQuery>();
            if (searchQuery == null)
                return Task.CompletedTask;

            if (!String.IsNullOrEmpty(searchQuery.Filter))
                ctx.Filter &= new QueryStringQuery {
                    Query = searchQuery.Filter,
                    DefaultOperator = Operator.And,
                    AnalyzeWildcard = false
                };

            if (!String.IsNullOrEmpty(searchQuery.Criteria))
                ctx.Query &= new QueryStringQuery {
                    Query = searchQuery.Criteria,
                    DefaultOperator = searchQuery.DefaultCriteriaOperator == SearchOperator.Or ? Operator.Or : Operator.And,
                    AnalyzeWildcard = true
                };

            if (!String.IsNullOrEmpty(searchQuery.Sort)) {
                var result = _parser.Parse(searchQuery.Sort);
                var opt = ctx.GetOptionsAs<IElasticQueryOptions>();
                TermToFieldVisitor.Run(result, ctx);
                var fields = GetReferencedFieldsQueryVisitor.Run(result);
                // TODO: Check referenced fields against opt.AllowedSortFields

                var sort = GetSortFieldsVisitor.Run(result, ctx);
                ctx.Search.Sort(sort);
            }

            return Task.CompletedTask;
        }
    }

    public class ParsedSearchQueryBuilder : IElasticQueryBuilder {
        private readonly ElasticQueryParser _parser;

        public ParsedSearchQueryBuilder(ElasticQueryParser parser = null) {
            _parser = parser ?? new ElasticQueryParser();
        }

        public async Task BuildAsync<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var searchQuery = ctx.GetSourceAs<ISearchQuery>();
            if (searchQuery == null)
                return;

            if (!String.IsNullOrEmpty(searchQuery.Criteria))
                ctx.Query &= await _parser.BuildQueryAsync(searchQuery.Criteria, ctx.SetDefaultOperator(Operator.Or).UseScoring()).AnyContext();

            // NOTE: Calling UseScoring here to keep the query from being wrapped in a filter which happens ElasticQueryBuilderExtensions.BuildQuery
            if (!String.IsNullOrEmpty(searchQuery.Filter))
                ctx.Filter &= await _parser.BuildQueryAsync(searchQuery.Filter, ctx.SetDefaultOperator(Operator.And).UseScoring()).AnyContext();

            if (!String.IsNullOrEmpty(searchQuery.Sort))
                ctx.Search.Sort(await _parser.BuildSortAsync(searchQuery.Sort, ctx).AnyContext());
        }
    }

    public static class SearchQueryExtensions {
        public static T WithFilter<T>(this T query, string filter) where T : ISearchQuery {
            query.Filter = filter;
            return query;
        }

        public static T WithSearchQuery<T>(this T query, string queryString, bool useAndAsDefaultOperator = true) where T : ISearchQuery {
            query.Criteria = queryString;
            query.DefaultCriteriaOperator = useAndAsDefaultOperator ? SearchOperator.And : SearchOperator.Or;
            return query;
        }

        public static T WithSort<T>(this T query, string sort) where T : ISearchQuery {
            query.Sort = sort;
            return query;
        }
    }
}