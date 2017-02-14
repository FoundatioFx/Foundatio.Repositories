using System;
using Exceptionless.DateTimeExtensions;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories {
    public static class SetElasticPagingOptionsExtensions {
        internal const string SnapshotPagingKey = "@SnapshotPaging";
        internal const string ScrollIdKey = "@ScrollId";
        internal const string SnapshotLifetimeKey = "@SnapshotLifetime";

        public static T UseSnapshotPaging<T>(this T options, bool useSnapshotPaging = true, TimeSpan? lifetime = null) where T : ICommandOptions {
            options.SetOption(SnapshotPagingKey, useSnapshotPaging);
            if (lifetime.HasValue)
                options.SetOption(SnapshotLifetimeKey, lifetime.Value);

            return options;
        }

        public static T WithScrollId<T>(this T options, string scrollId) where T : ICommandOptions {
            options.SetOption(SnapshotPagingKey, true);
            options.SetOption(ScrollIdKey, scrollId);
            return options;
        }

        public static T WithScrollId<T>(this T options, IHaveData results) where T : ICommandOptions {
            options.SetOption(SnapshotPagingKey, true);
            options.SetOption(ScrollIdKey, results.GetScrollId());
            return options;
        }

        public static string GetScrollId(this IHaveData results) {
            return results.Data.GetString(ElasticDataKeys.ScrollId);
        }
    }
}

namespace Foundatio.Repositories.Options {
    public static class ReadElasticPagingOptionsExtensions {
        public static string GetLifetime<T>(this T options) where T : ICommandOptions {
            return options.GetOption(SetElasticPagingOptionsExtensions.SnapshotLifetimeKey, TimeSpan.FromMinutes(2)).ToWords(true, 1);
        }

        public static bool ShouldUseSnapshotPaging<T>(this T options) where T : ICommandOptions {
            return options.GetOption(SetElasticPagingOptionsExtensions.SnapshotPagingKey, false);
        }
    }
}
