using System;
using Foundatio.Repositories.Options;

namespace Foundatio.Repositories {
    public static class AsyncQueryOptionsExtensions {
        public static T AsyncQuery<T>(this T options, TimeSpan? waitTime = null, TimeSpan? ttl = null) where T : ICommandOptions {
            options.BuildOption(AsyncQueryEnabledKey, true);
            
            if (waitTime.HasValue)
                options.AsyncQueryWaitTime(waitTime.Value);
            
            if (ttl.HasValue)
                options.AsyncQueryTtl(ttl.Value);

            return options;
        }

        internal const string AsyncQueryEnabledKey = "@AsyncQueryEnabled";
        public static T AsyncQueryEnabled<T>(this T options, bool enabled = true) where T : ICommandOptions {
            return options.BuildOption(AsyncQueryEnabledKey, enabled);
        }

        internal const string AsyncQueryWaitTimeKey = "@AsyncQueryTtl";
        public static T AsyncQueryWaitTime<T>(this T options, TimeSpan waitTime) where T : ICommandOptions {
            return options.BuildOption(AsyncQueryWaitTimeKey, waitTime);
        }

        internal const string AsyncQueryTtlKey = "@AsyncQueryTtl";
        public static T AsyncQueryTtl<T>(this T options, TimeSpan timeout) where T : ICommandOptions {
            return options.BuildOption(AsyncQueryTtlKey, timeout);
        }

        internal const string AsyncQueryIdKey = "@AsyncQueryId";
        internal const string AsyncQueryAutoDeleteKey = "@AsyncQueryAutoDelete";
        public static T AsyncQueryId<T>(this T options, string id, TimeSpan? waitTime = null, bool autoDelete = false) where T : ICommandOptions {
            options.BuildOption(AsyncQueryIdKey, id);

            if (waitTime.HasValue)
                options.AsyncQueryWaitTime(waitTime.Value);

            options.BuildOption(AsyncQueryAutoDeleteKey, autoDelete);

            return options;
        }
    }
}

namespace Foundatio.Repositories.Options {
    public static class ReadAsyncQueryOptionsExtensions {
        public static bool ShouldUseAsyncQuery(this ICommandOptions options) {
            return options.SafeGetOption<bool>(AsyncQueryOptionsExtensions.AsyncQueryEnabledKey, false);
        }

        public static bool HasAsyncQueryWaitTime(this ICommandOptions options) {
            return options.SafeHasOption(AsyncQueryOptionsExtensions.AsyncQueryWaitTimeKey);
        }

        public static TimeSpan GetAsyncQueryWaitTime(this ICommandOptions options) {
            return options.SafeGetOption<TimeSpan>(AsyncQueryOptionsExtensions.AsyncQueryWaitTimeKey, TimeSpan.FromSeconds(1));
        }

        public static bool HasAsyncQueryTtl(this ICommandOptions options) {
            return options.SafeHasOption(AsyncQueryOptionsExtensions.AsyncQueryTtlKey);
        }

        public static TimeSpan GetAsyncQueryTtl(this ICommandOptions options) {
            return options.SafeGetOption<TimeSpan>(AsyncQueryOptionsExtensions.AsyncQueryTtlKey, TimeSpan.FromDays(1));
        }

        public static bool HasAsyncQueryId(this ICommandOptions options) {
            return options.SafeHasOption(AsyncQueryOptionsExtensions.AsyncQueryIdKey);
        }

        public static string GetAsyncQueryId(this ICommandOptions options) {
            return options.SafeGetOption<string>(AsyncQueryOptionsExtensions.AsyncQueryIdKey, null);
        }

        public static bool ShouldAutoDeleteAsyncQuery(this ICommandOptions options) {
            return options.SafeGetOption<bool>(AsyncQueryOptionsExtensions.AsyncQueryAutoDeleteKey, false);
        }
    }
}
