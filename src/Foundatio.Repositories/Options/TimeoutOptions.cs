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

        /// <summary>
        /// Sets how many times a write is retried after a version conflict or a transient failure, in addition to the
        /// first attempt. <c>Retry(0)</c> makes a single attempt and fails fast. Defaults to 10 retries when not set.
        /// </summary>
        /// <remarks>
        /// The same budget applies everywhere: Elasticsearch <c>retry_on_conflict</c> for script and partial patches,
        /// client-side re-read and re-apply rounds for JSON and action patches, delete-by-query passes, and retries
        /// of transient errors (HTTP 429, 502, 503 and 504) for single-document writes.
        /// </remarks>
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

        /// <summary>
        /// Gets the number of retries (not attempts) configured with <see cref="TimeoutOptionsExtensions.Retry{T}(T, int)"/>.
        /// Defaults to 10. Negative values are treated as 0.
        /// </summary>
        public static int GetRetryCount(this ICommandOptions options, int defaultRetryCount = 10)
        {
            return Math.Max(0, options.SafeGetOption<int>(TimeoutOptionsExtensions.RetryCountKey, defaultRetryCount));
        }
    }
}
