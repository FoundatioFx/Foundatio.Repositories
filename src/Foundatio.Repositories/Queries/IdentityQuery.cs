using System.Collections.Generic;
using Foundatio.Repositories.Options;

namespace Foundatio.Repositories {
    public static class IdentityQueryExtensions {
        internal const string IdsKey = "@Ids";

        public static T Id<T>(this T query, string id) where T : IRepositoryQuery {
            return query.AddSetOptionValue(IdsKey, id);
        }

        public static T Id<T>(this T query, params string[] ids) where T : IRepositoryQuery {
            return query.AddSetOptionValue(IdsKey, ids);
        }

        public static T Id<T>(this T query, IEnumerable<string> ids) where T : IRepositoryQuery {
            return query.AddSetOptionValue(IdsKey, ids);
        }

        internal const string OnlyIdsKey = "@OnlyIds";

        public static T OnlyIds<T>(this T query) where T : IRepositoryQuery {
            return query.BuildOption(OnlyIdsKey, true);
        }

        internal const string ExcludedIdsKey = "@ExcludedIds";
        public static T ExcludedId<T>(this T query, string id) where T : IRepositoryQuery {
            return query.AddSetOptionValue(ExcludedIdsKey, id);
        }

        public static T ExcludedId<T>(this T query, params string[] ids) where T : IRepositoryQuery {
            return query.AddSetOptionValue(ExcludedIdsKey, ids);
        }

        public static T ExcludedId<T>(this T query, IEnumerable<string> ids) where T : IRepositoryQuery {
            return query.AddSetOptionValue(ExcludedIdsKey, ids);
        }
    }
}

namespace Foundatio.Repositories.Queries {
    public static class ReadIdentityQueryExtensions {
        public static ISet<string> GetIds<T>(this T options) where T : IRepositoryQuery {
            return options.SafeGetSet<string>(IdentityQueryExtensions.IdsKey);
        }

        public static bool ShouldOnlyHaveIds<T>(this T options) where T : IRepositoryQuery {
            return options.SafeGetOption<bool>(IdentityQueryExtensions.OnlyIdsKey);
        }

        public static ISet<string> GetExcludedIds<T>(this T options) where T : IRepositoryQuery {
            return options.SafeGetSet<string>(IdentityQueryExtensions.ExcludedIdsKey);
        }
    }
}
