using System;
using ElasticMacros;
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

    public class SearchQueryBuilder : IElasticQueryBuilder {
        public SearchQueryBuilder() {
            new ElasticMacroProcessor();
        }

        public void Build<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var searchQuery = ctx.GetSourceAs<ISearchQuery>();
            if (searchQuery == null)
                return;

            var processor = new ElasticMacroProcessor(c => c
                .UseGeo(l => "d", "field4")
                .UseAliases(name => name == "geo" ? "field4" : name));
            var filterContainer = processor.Process("geo:[9 TO d] OR field1:value1 OR field2:[1 TO 4] OR -geo:\"Dallas, TX\"~75mi");

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
}