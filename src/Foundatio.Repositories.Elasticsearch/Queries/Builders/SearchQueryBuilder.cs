using System;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public interface ISearchQuery {
        string Filter { get; set; }
        string Criteria { get; set; }
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

        public void Build<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var searchQuery = ctx.GetSourceAs<ISearchQuery>();
            if (searchQuery == null)
                return;

            if (!String.IsNullOrEmpty(searchQuery.Filter)) {
                var result = _parser.Parse(searchQuery.Filter);
                searchQuery.Filter = GenerateQueryVisitor.Run(AliasedQueryVisitor.Run(result, _aliasMap));

                ctx.Filter &= new QueryStringQuery {
                    Query = searchQuery.Filter,
                    DefaultOperator = Operator.And,
                    AnalyzeWildcard = false
                };
            }

            if (!String.IsNullOrEmpty(searchQuery.Criteria)) {
                var result = _parser.Parse(searchQuery.Criteria);
                searchQuery.Criteria = GenerateQueryVisitor.Run(AliasedQueryVisitor.Run(result, _aliasMap));

                ctx.Query &= new QueryStringQuery {
                    Query = searchQuery.Criteria,
                    DefaultOperator = searchQuery.DefaultCriteriaOperator == SearchOperator.Or ? Operator.Or : Operator.And,
                    AnalyzeWildcard = true
                };
            }
        }
    }

    public class SearchQueryBuilder : IElasticQueryBuilder {
        public void Build<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var searchQuery = ctx.GetSourceAs<ISearchQuery>();
            if (searchQuery == null)
                return;

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
        }
    }

    public class ParsedSearchQueryBuilder : IElasticQueryBuilder {
        private readonly ElasticQueryParser _parser;

        public ParsedSearchQueryBuilder(ElasticQueryParser parser = null) {
            _parser = parser ?? new ElasticQueryParser();
        }

        public void Build<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var searchQuery = ctx.GetSourceAs<ISearchQuery>();
            if (searchQuery == null)
                return;

            // TODO: Use default search operator and wildcards
            if (!String.IsNullOrEmpty(searchQuery.Criteria))
                ctx.Query &= _parser.BuildQuery(searchQuery.Criteria, defaultOperator: Operator.Or, scoreResults: true);

            if (!String.IsNullOrEmpty(searchQuery.Filter))
                ctx.Filter &= _parser.BuildQuery(searchQuery.Filter);
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
    }
}