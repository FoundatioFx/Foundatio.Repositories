namespace Foundatio.Repositories {
    public static class QueryLogOptionsExtensions {
        internal const string QueryLogLevelKey = "@QueryLogLevel";
        public static T QueryLogLevel<T>(this T options, Microsoft.Extensions.Logging.LogLevel logLevel) where T : ICommandOptions {
            options.Values.Set(QueryLogLevelKey, logLevel);
            return options;
        }
    }
}

namespace Foundatio.Repositories.Options {
    public static class ReadQueryLogOptionsExtensions {
        public static Microsoft.Extensions.Logging.LogLevel GetQueryLogLevel(this ICommandOptions options, Microsoft.Extensions.Logging.LogLevel defaultLogLevel = Microsoft.Extensions.Logging.LogLevel.Trace) {
            return options.SafeGetOption(QueryLogOptionsExtensions.QueryLogLevelKey, defaultLogLevel);
        }
    }
}

