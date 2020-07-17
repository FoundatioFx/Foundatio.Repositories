using Foundatio.Repositories.Options;

namespace Foundatio.Repositories {
    public static class SoftDeleteOptionsExtensions {
        internal const string SoftDeleteModeKey = "@SoftDeleteMode";

        public static T SoftDeleteMode<T>(this T query, SoftDeleteQueryMode mode) where T : ICommandOptions {
            return query.BuildOption(SoftDeleteModeKey, mode);
        }

        public static T IncludeSoftDeletes<T>(this T query, bool includeDeleted = true) where T : ICommandOptions {
            return query.BuildOption(SoftDeleteModeKey, includeDeleted ? SoftDeleteQueryMode.All : SoftDeleteQueryMode.ActiveOnly);
        }
    }

    public enum SoftDeleteQueryMode {
        ActiveOnly,
        DeletedOnly,
        All
    }
}

namespace Foundatio.Repositories.Options {
    public static class ReadSoftDeleteOptionsExtensions {
        public static SoftDeleteQueryMode GetSoftDeleteMode(this ICommandOptions options, SoftDeleteQueryMode defaultMode = SoftDeleteQueryMode.ActiveOnly) {
            return options.SafeGetOption<SoftDeleteQueryMode>(SoftDeleteOptionsExtensions.SoftDeleteModeKey, defaultMode);
        }
    }
}
