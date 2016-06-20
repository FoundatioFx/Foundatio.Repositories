using System;
using System.Collections.Generic;

namespace Foundatio.Repositories.Elasticsearch.Queries {
    public interface IElasticIndexesQuery {
        List<string> Indices { get; set; }
        DateTime? UtcStart { get; set; }
        DateTime? UtcEnd { get; set; }
    }

    public static class ElasticFilterIndicesExtensions {
        public static T WithIndice<T>(this T query, string index) where T : IElasticIndexesQuery {
            query.Indices?.Add(index);
            return query;
        }

        public static T WithIndices<T>(this T query, params string[] indices) where T : IElasticIndexesQuery {
            query.Indices?.AddRange(indices);
            return query;
        }
        
        public static T WithIndices<T>(this T query, IEnumerable<string> indices) where T : IElasticIndexesQuery {
            query.Indices?.AddRange(indices);
            return query;
        }

        public static T WithIndices<T>(this T query, DateTime? utcStart, DateTime? utcEnd) where T : IElasticIndexesQuery {
            query.UtcStart = utcStart;
            query.UtcEnd = utcEnd;

            return query;
        }
    }
}