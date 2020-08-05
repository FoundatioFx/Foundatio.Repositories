namespace Foundatio.Repositories {
    public static class QueryLogOptionsExtensions {
        internal const string QueryLogLevelKey = "@QueryLogLevel";
        public static T QueryLogLevel<T>(this T options, Microsoft.Extensions.Logging.LogLevel logLevel) where T : ICommandOptions {
            options.Values.Set(QueryLogLevelKey, logLevel);
            return options;
        }

        internal const string DefaultQueryLogLevelKey = "@DefaultQueryLogLevel";
        public static T DefaultQueryLogLevel<T>(this T options, Microsoft.Extensions.Logging.LogLevel logLevel) where T : ICommandOptions {
            options.Values.Set(DefaultQueryLogLevelKey, logLevel);

            return options;
        }
    }
}

namespace Foundatio.Repositories.Options {
    public static class ReadQueryLogOptionsExtensions {
        public static Microsoft.Extensions.Logging.LogLevel GetQueryLogLevel(this ICommandOptions options) {
            return options.SafeGetOption(QueryLogOptionsExtensions.QueryLogLevelKey, options.SafeGetOption(QueryLogOptionsExtensions.DefaultQueryLogLevelKey, Microsoft.Extensions.Logging.LogLevel.Trace));
        }
    }
}

