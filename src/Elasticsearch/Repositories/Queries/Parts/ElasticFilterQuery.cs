using System;
using Nest;

namespace Foundatio.Elasticsearch.Repositories.Queries {
    public interface IElasticFilterQuery {
        QueryContainer ElasticFilter { get; set; }
    }

    public static class ElasticFilterQueryExtensions {
        public static T WithElasticFilter<T>(this T query, QueryContainer filter) where T : IElasticFilterQuery {
            query.ElasticFilter = filter;
            return query;
        }
    }
}