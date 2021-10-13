using System;
using Foundatio.Repositories.Options;

namespace Foundatio.Repositories {
    public static class TimeoutOptionsExtensions {

        internal const string QueryTimeoutKey = "@QueryTimeout";
        public static T Timeout<T>(this T options, TimeSpan timeout) where T : ICommandOptions {
            return options.BuildOption(QueryTimeoutKey, timeout);
        }
    }
}

namespace Foundatio.Repositories.Options {
    public static class ReadTimeoutOptionsExtensions {
        public static bool HasQueryTimeout(this ICommandOptions options) {
            return options.SafeHasOption(TimeoutOptionsExtensions.QueryTimeoutKey);
        }

        public static TimeSpan GetQueryTimeout(this ICommandOptions options) {
            return options.SafeGetOption<TimeSpan>(TimeoutOptionsExtensions.QueryTimeoutKey, TimeSpan.FromSeconds(1));
        }
    }
}
