using System;
using Foundatio.Repositories.Elasticsearch.Repositories;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories.Elasticsearch.Models {
    public class ElasticPagingOptions: PagingOptions {
        public bool UseSnapshotPaging { get; set; }
        public string ScrollId { get; set; }
        public TimeSpan? SnapshotLifetime { get; set; }

        public static ElasticPagingOptions FromOptions(IPagingOptions options) {
            var elasticOptions = new ElasticPagingOptions();
            if (options == null)
                return elasticOptions;

            elasticOptions.Page = options.Page;
            elasticOptions.Limit = options.Limit;

            return elasticOptions;
        }
    }

    public static class PagingOptionsExtensions {
        public static ElasticPagingOptions WithLimit(this ElasticPagingOptions options, int? limit) {
            options.Limit = limit;
            return options;
        }

        public static ElasticPagingOptions WithPage(this ElasticPagingOptions options, int? page) {
            options.Page = page;
            return options;
        }

        public static ElasticPagingOptions UseSnapshotPaging(this ElasticPagingOptions options, bool useSnapshotPaging = true) {
            options.UseSnapshotPaging = useSnapshotPaging;
            return options;
        }

        public static ElasticPagingOptions WithScrollId(this ElasticPagingOptions options, string scrollId) {
            options.UseSnapshotPaging = true;
            options.ScrollId = scrollId;
            return options;
        }

        public static ElasticPagingOptions WithScrollId<T>(this ElasticPagingOptions options, IFindResults<T> results) where T : class {
            var elasticResults = results as IElasticFindResults<T>;
            if (elasticResults == null)
                return options;

            options.UseSnapshotPaging = true;
            options.ScrollId = elasticResults.ScrollId;
            return options;
        }

        public static ElasticPagingOptions WithSnapshotLifetime(this ElasticPagingOptions options, TimeSpan lifetime) {
            options.UseSnapshotPaging = true;
            options.SnapshotLifetime = lifetime;
            return options;
        }
    }
}
