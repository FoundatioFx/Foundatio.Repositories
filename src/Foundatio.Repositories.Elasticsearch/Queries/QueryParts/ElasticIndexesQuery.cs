using System;
using System.Collections.Generic;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Queries;

namespace Foundatio.Repositories.Elasticsearch.Queries {
    public interface IElasticIndexesQuery : IRepositoryQuery {
        ICollection<string> Indexes { get; set; }
        DateTime? UtcStartIndex { get; set; }
        DateTime? UtcEndIndex { get; set; }
    }

    public static class ElasticIndexesQueryExtensions {
        public static T WithIndex<T>(this T query, string index) where T : IElasticIndexesQuery {
            query.Indexes?.Add(index);
            return query;
        }

        public static T WithIndexes<T>(this T query, params string[] indexes) where T : IElasticIndexesQuery {
            query.Indexes?.AddRange(indexes);
            return query;
        }
        
        public static T WithIndexes<T>(this T query, IEnumerable<string> indexes) where T : IElasticIndexesQuery {
            query.Indexes?.AddRange(indexes);
            return query;
        }

        public static T WithIndexes<T>(this T query, DateTime? utcStart, DateTime? utcEnd) where T : IElasticIndexesQuery {
            query.UtcStartIndex = utcStart;
            query.UtcEndIndex = utcEnd;

            return query;
        }
    }
}