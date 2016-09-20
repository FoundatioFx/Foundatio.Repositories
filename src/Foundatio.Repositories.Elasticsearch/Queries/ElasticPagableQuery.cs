using System;
using Exceptionless.DateTimeExtensions;
using Foundatio.Repositories.Elasticsearch.Models;
using Foundatio.Repositories.Queries;

namespace Foundatio.Repositories.Elasticsearch.Queries {
    public static class PagableQueryExtensions {
        public static string GetLifetime<T>(this T query) where T : IPagableQuery {
            var elasticPagableQuery = query?.Options as ElasticPagingOptions;
            if (elasticPagableQuery == null)
                return "2m";

            return elasticPagableQuery.SnapshotLifetime.HasValue ? elasticPagableQuery.SnapshotLifetime.Value.ToWords(true, 1) : "2m";
        }

        public static bool ShouldUseSnapshotPaging<T>(this T query) where T : IPagableQuery {
            var elasticPagableQuery = query?.Options as ElasticPagingOptions;
            return elasticPagableQuery != null && elasticPagableQuery.UseSnapshotPaging;
        }

        public static T WithSnapshotPaging<T>(this T query, bool useSnapshotPaging = true) where T : IPagableQuery {
            if (query == null)
                return query;

            var elasticOptions = query.Options as ElasticPagingOptions;
            if (elasticOptions == null) {
                elasticOptions = ElasticPagingOptions.FromOptions(query.Options);
                query.Options = elasticOptions;
            }

            elasticOptions.UseSnapshotPaging = useSnapshotPaging;
            return query;
        }

        public static T WithSnapshotLifetime<T>(this T query, TimeSpan lifetime) where T : IPagableQuery {
            if (query == null)
                return query;

            var elasticOptions = query.Options as ElasticPagingOptions;
            if (elasticOptions == null) {
                elasticOptions = ElasticPagingOptions.FromOptions(query.Options);
                query.Options = elasticOptions;
            }

            elasticOptions.SnapshotLifetime = lifetime;
            return query;
        }
    }
}
