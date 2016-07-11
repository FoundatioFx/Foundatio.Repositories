using System;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Queries.Builders {
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