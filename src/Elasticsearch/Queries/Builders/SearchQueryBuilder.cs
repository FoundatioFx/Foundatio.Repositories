using System;
using ElasticMacros;

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
        private readonly ElasticMacroProcessor _processor;

        public SearchQueryBuilder(ElasticMacroProcessor processor) {
            _processor = processor ?? new ElasticMacroProcessor();
        }

        public void Build<T>(QueryBuilderContext<T> ctx) where T : class, new() {
            var searchQuery = ctx.GetSourceAs<ISearchQuery>();
            if (searchQuery == null)
                return;

            // TODO: Use default search operator and wildcards
            if (!String.IsNullOrEmpty(searchQuery.SearchQuery))
                ctx.Query &= _processor.ProcessQuery(searchQuery.SearchQuery);

            if (!String.IsNullOrEmpty(searchQuery.Filter))
                ctx.Filter &= _processor.ProcessFilter(searchQuery.Filter);
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