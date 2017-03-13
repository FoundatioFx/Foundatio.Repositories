using System;
using System.Collections.Generic;
using Foundatio.Repositories.Options;

namespace Foundatio.Repositories {
    public static class ElasticIndexesQueryExtensions {
        internal const string ElasticIndexesKey = "@ElasticIndexes";
        public static T Index<T>(this T query, string field) where T : IRepositoryQuery {
            return query.AddCollectionOptionValue(ElasticIndexesKey, field);
        }

        public static T Index<T>(this T query, IEnumerable<string> fields) where T : IRepositoryQuery {
            return query.AddCollectionOptionValue(ElasticIndexesKey, fields);
        }

        public static T Index<T>(this T query, params string[] fields) where T : IRepositoryQuery {
            return query.AddCollectionOptionValue(ElasticIndexesKey, fields);
        }

        internal const string ElasticIndexesStartUtcKey = "@ElasticIndexesStartUtc";
        internal const string ElasticIndexesEndUtcKey = "@ElasticIndexesEndUtc";
        public static T Index<T>(this T query, DateTime? utcStart, DateTime? utcEnd) where T : IRepositoryQuery {
            if (utcStart.HasValue)
                query.Values.Set(ElasticIndexesStartUtcKey, utcStart);

            if (utcEnd.HasValue)
                query.Values.Set(ElasticIndexesEndUtcKey, utcEnd);

            return query;
        }
    }
}

namespace Foundatio.Repositories.Options {
    public static class ReadElasticIndexesQueryExtensions {
        public static ICollection<string> GetElasticIndexes(this IRepositoryQuery options) {
            return options.SafeGetCollection<string>(ElasticIndexesQueryExtensions.ElasticIndexesKey);
        }

        public static DateTime? GetElasticIndexesStartUtc(this IRepositoryQuery options) {
            return options.SafeGetOption<DateTime?>(ElasticIndexesQueryExtensions.ElasticIndexesStartUtcKey);
        }

        public static DateTime? GetElasticIndexesEndUtc(this IRepositoryQuery options) {
            return options.SafeGetOption<DateTime?>(ElasticIndexesQueryExtensions.ElasticIndexesEndUtcKey);
        }
    }
}
