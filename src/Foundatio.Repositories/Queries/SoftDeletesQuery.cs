using Foundatio.Repositories.Options;

namespace Foundatio.Repositories {
    public static class SoftDeleteQueryExtensions {
        internal const string SoftDeleteModeKey = "@SoftDeleteMode";

        public static T SoftDeleteMode<T>(this T query, SoftDeleteQueryMode mode) where T : IRepositoryQuery {
            return query.BuildOption(SoftDeleteModeKey, mode);
        }
    }

    public enum SoftDeleteQueryMode {
        ActiveOnly,
        DeletedOnly,
        All
    }
}

namespace Foundatio.Repositories.Queries {
    public static class ReadCacheOptionsExtensions {
        public static SoftDeleteQueryMode GetSoftDeleteMode(this IRepositoryQuery query, SoftDeleteQueryMode defaultMode = SoftDeleteQueryMode.ActiveOnly) {
            return query.SafeGetOption<SoftDeleteQueryMode>(SoftDeleteQueryExtensions.SoftDeleteModeKey, defaultMode);
        }
    }
}
