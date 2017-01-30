using System;
using Exceptionless.DateTimeExtensions;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;

namespace Foundatio.Repositories.Elasticsearch.Models {
    public interface IElasticPagingOptions : IPagingOptions {
        bool UseSnapshotPaging { get; set; }
        string ScrollId { get; set; }
        TimeSpan? SnapshotLifetime { get; set; }
    }

    public class ElasticPagingOptions: PagingOptions, IElasticPagingOptions {
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
        public static string GetLifetime<T>(this T options) where T : ICommandOptions {
            var elasticPagingOptions = options as IElasticPagingOptions;
            if (elasticPagingOptions == null)
                return "2m";

            return elasticPagingOptions.SnapshotLifetime.HasValue ? elasticPagingOptions.SnapshotLifetime.Value.ToWords(true, 1) : "2m";
        }

        public static bool ShouldUseSnapshotPaging<T>(this T options) where T : ICommandOptions {
            var elasticPagingOptions = options as IElasticPagingOptions;
            return elasticPagingOptions != null && elasticPagingOptions.UseSnapshotPaging;
        }

        public static T UseSnapshotPaging<T>(this T options, bool useSnapshotPaging = true) where T : IElasticPagingOptions {
            options.UseSnapshotPaging = useSnapshotPaging;
            return options;
        }

        public static T WithSnapshotLifetime<T>(this T options, TimeSpan lifetime) where T : IElasticPagingOptions {
            options.UseSnapshotPaging = true;
            options.SnapshotLifetime = lifetime;
            return options;
        }

        public static T WithScrollId<T>(this T options, string scrollId) where T : IElasticPagingOptions {
            options.UseSnapshotPaging = true;
            options.ScrollId = scrollId;
            return options;
        }

        public static T WithScrollId<T>(this T options, IHaveData results) where T : IElasticPagingOptions {
            options.UseSnapshotPaging = true;
            options.ScrollId = results.GetScrollId();
            return options;
        }

        public static string GetScrollId(this IHaveData results) {
            return results.Data.GetString(ElasticDataKeys.ScrollId);
        }
    }
}
