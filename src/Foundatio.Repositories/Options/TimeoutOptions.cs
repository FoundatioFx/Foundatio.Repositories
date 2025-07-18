using System;
using Foundatio.Repositories.Options;

namespace Foundatio.Repositories
{
    public static class TimeoutOptionsExtensions
    {
        internal const string QueryTimeoutKey = "@QueryTimeout";
        public static T Timeout<T>(this T options, TimeSpan timeout) where T : ICommandOptions
        {
            return options.BuildOption(QueryTimeoutKey, timeout);
        }

        internal const string RetryCountKey = "@RetryCount";
        public static T Retry<T>(this T options, int retryCount) where T : ICommandOptions
        {
            return options.BuildOption(RetryCountKey, retryCount);
        }
    }
}

namespace Foundatio.Repositories.Options
{
    public static class ReadTimeoutOptionsExtensions
    {
        public static bool HasQueryTimeout(this ICommandOptions options)
        {
            return options.SafeHasOption(TimeoutOptionsExtensions.QueryTimeoutKey);
        }

        public static TimeSpan GetQueryTimeout(this ICommandOptions options)
        {
            return options.SafeGetOption<TimeSpan>(TimeoutOptionsExtensions.QueryTimeoutKey, TimeSpan.FromSeconds(1));
        }

        public static bool HasRetryCount(this ICommandOptions options)
        {
            return options.SafeHasOption(TimeoutOptionsExtensions.RetryCountKey);
        }

        public static int GetRetryCount(this ICommandOptions options, int defaultRetryCount = 10)
        {
            return options.SafeGetOption<int>(TimeoutOptionsExtensions.RetryCountKey, defaultRetryCount);
        }
    }
}
