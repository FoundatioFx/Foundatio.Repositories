using System;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
    public interface ISearchQuery {
        string Filter { get; set; }
        string SearchQuery { get; set; }
        SearchOperator DefaultSearchQueryOperator { get; set; }
    }

    public enum SearchOperator {
        And,
        Or
    }

    public static class SearchQueryExtensions {
        public static T WithFilter<T>(this T query, string filter) where T : ISearchQuery {
            query.Filter = filter;
            return query;
        }

        public static T WithSearchQuery<T>(this T query, string queryString, bool useAndAsDefaultOperator = true) where T : ISearchQuery {
            query.SearchQuery = queryString;
            query.DefaultSearchQueryOperator = useAndAsDefaultOperator ? SearchOperator.And : SearchOperator.Or;
            return query;
        }
    }

    public class SearchQueryBuilder : IElasticQueryBuilder {
        public void Build<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var searchQuery = ctx.GetQueryAs<ISearchQuery>();
            if (searchQuery == null)
                return;

            if (!String.IsNullOrEmpty(searchQuery.SearchQuery)) {
                ctx.Query &= new QueryStringQuery {
                    Query = searchQuery.SearchQuery,
                    DefaultOperator = searchQuery.DefaultSearchQueryOperator == SearchOperator.Or ? Operator.Or : Operator.And,
                    AnalyzeWildcard = true
                };
            }

            if (!String.IsNullOrEmpty(searchQuery.Filter)) {
                ctx.Filter &= new QueryFilter {
                    Query = QueryContainer.From(new QueryStringQuery {
                        Query = searchQuery.Filter,
                        DefaultOperator = Operator.And
                    })
                };
            }
        }
    }
}