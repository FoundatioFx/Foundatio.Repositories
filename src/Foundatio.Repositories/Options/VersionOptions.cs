using Foundatio.Repositories.Options;

namespace Foundatio.Repositories {
    public static class SetVersionsOptionsExtensions {
        internal const string SkipVersionCheckKey = "@SkipVersionCheck";

        public static T VersionCheck<T>(this T options, bool shouldCheckVersion) where T : ICommandOptions {
            return options.BuildOption(SkipVersionCheckKey, shouldCheckVersion);
        }

        public static T SkipVersionCheck<T>(this T options) where T : ICommandOptions {
            return options.VersionCheck(true);
        }
    }
}

namespace Foundatio.Repositories.Options {
    public static class ReadVersionOptionsExtensions {
        public static bool ShouldSkipVersionCheck(this ICommandOptions options) {
            return options.SafeGetOption(SetVersionsOptionsExtensions.SkipVersionCheckKey, false);
        }
    }
}
