using System;
using Foundatio.Repositories.Options;

namespace Foundatio.Repositories {
    public static class SoftDeleteQueryExtensions {
        internal const string SoftDeleteModeKey = "@SoftDeleteMode";

        public static T IncludeDeleted<T>(this T query, bool includeDeleted = true) where T : IRepositoryQuery {
            return query.BuildOption(SoftDeleteModeKey, includeDeleted ? SoftDeleteQueryMode.All : SoftDeleteQueryMode.ActiveOnly);
        }

        public static T IncludeOnlyDeleted<T>(this T query) where T : IRepositoryQuery {
            return query.BuildOption(SoftDeleteModeKey, SoftDeleteQueryMode.DeletedOnly);
        }

        public static T WithSoftDeleteMode<T>(this T query, SoftDeleteQueryMode mode) where T : IRepositoryQuery {
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
        public static SoftDeleteQueryMode GetSoftDeleteMode<T>(this T options) where T : IRepositoryQuery {
            return options.SafeGetOption<SoftDeleteQueryMode>(SoftDeleteQueryExtensions.SoftDeleteModeKey, SoftDeleteQueryMode.ActiveOnly);
        }
    }
}
