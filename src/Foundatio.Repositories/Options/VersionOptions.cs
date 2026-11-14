using System;
using Foundatio.Repositories.Options;

namespace Foundatio.Repositories
{
    public static class SetVersionsOptionsExtensions
    {
        internal const string SkipVersionCheckKey = "@SkipVersionCheck";
        internal const string ExpectedVersionKey = "@ExpectedVersion";

        /// <summary>
        /// Sets whether optimistic concurrency checks run for this operation.
        /// <c>true</c> (the default) checks the document version; <c>false</c> skips the check and overwrites the stored document.
        /// </summary>
        public static T VersionCheck<T>(this T options, bool shouldCheckVersion) where T : ICommandOptions
        {
            return options.BuildOption(SkipVersionCheckKey, !shouldCheckVersion);
        }

        /// <summary>
        /// Skips the optimistic concurrency check, so the write replaces the stored document regardless of its current version.
        /// </summary>
        public static T SkipVersionCheck<T>(this T options) where T : ICommandOptions
        {
            return options.VersionCheck(false);
        }

        /// <summary>
        /// Requires the stored document to have the given version (<c>"{primaryTerm}:{sequenceNumber}"</c>) when patching
        /// a single document with <c>PatchAsync(Id)</c>. Every write attempt, including retries of transient failures, is
        /// sent with that version as <c>if_seq_no</c>/<c>if_primary_term</c>, and the document is never re-read, so the
        /// patch fails with a version conflict when the document has changed. It works for entities that don't implement
        /// <c>IVersioned</c>. It can't be combined with <see cref="SkipVersionCheck{T}"/> or used with a script patch and
        /// a default ingest pipeline. <c>PatchAsync(Ids)</c>, <c>PatchAllAsync</c>, <c>AddAsync</c>, <c>SaveAsync</c>,
        /// <c>RemoveAsync</c> and <c>RemoveAllAsync</c> reject it. A nonempty malformed or unassigned token throws
        /// <see cref="ArgumentException"/> when the patch is executed. Passing <c>null</c> or an empty string clears the expectation.
        /// </summary>
        public static T IfVersion<T>(this T options, string? version) where T : ICommandOptions
        {
            if (String.IsNullOrEmpty(version))
            {
                options.Values.Remove(ExpectedVersionKey);
                return options;
            }

            return options.BuildOption(ExpectedVersionKey, version);
        }
    }
}

namespace Foundatio.Repositories.Options
{
    public static class ReadVersionOptionsExtensions
    {
        public static bool ShouldSkipVersionCheck(this ICommandOptions options)
        {
            return options.SafeGetOption(SetVersionsOptionsExtensions.SkipVersionCheckKey, false);
        }

        public static bool HasExpectedVersion(this ICommandOptions options)
        {
            return !String.IsNullOrEmpty(options.GetExpectedVersion());
        }

        public static string? GetExpectedVersion(this ICommandOptions options)
        {
            return options.SafeGetOption<string?>(SetVersionsOptionsExtensions.ExpectedVersionKey, null);
        }
    }
}
