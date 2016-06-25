using System;
using System.Collections.Generic;

namespace Foundatio.Repositories.Elasticsearch.Queries {
    public interface IElasticIndexesQuery {
        List<string> Indexes { get; set; }
        DateTime? UtcStartIndex { get; set; }
        DateTime? UtcEndIndex { get; set; }
    }

    public static class ElasticFilterIndicesExtensions {
        public static T WithIndice<T>(this T query, string index) where T : IElasticIndexesQuery {
            query.Indexes?.Add(index);
            return query;
        }

        public static T WithIndices<T>(this T query, params string[] indices) where T : IElasticIndexesQuery {
            query.Indexes?.AddRange(indices);
            return query;
        }
        
        public static T WithIndices<T>(this T query, IEnumerable<string> indices) where T : IElasticIndexesQuery {
            query.Indexes?.AddRange(indices);
            return query;
        }

        public static T WithIndices<T>(this T query, DateTime? utcStart, DateTime? utcEnd) where T : IElasticIndexesQuery {
            query.UtcStartIndex = utcStart;
            query.UtcEndIndex = utcEnd;

            return query;
        }
    }
}