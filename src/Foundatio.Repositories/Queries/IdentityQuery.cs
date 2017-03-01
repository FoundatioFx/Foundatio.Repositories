using System;
using System.Collections.Generic;
using Foundatio.Repositories.Options;

namespace Foundatio.Repositories {
    public static class IdentityQueryExtensions {
        internal const string IdsKey = "@Ids";

        public static T WithId<T>(this T query, string id) where T : IRepositoryQuery {
            return query.AddSetOptionValue(IdsKey, id);
        }

        public static T WithIds<T>(this T query, params string[] ids) where T : IRepositoryQuery {
            return query.AddSetOptionValue(IdsKey, ids);
        }

        public static T WithIds<T>(this T query, IEnumerable<string> ids) where T : IRepositoryQuery {
            return query.AddSetOptionValue(IdsKey, ids);
        }

        internal const string ExcludedIdsKey = "@ExcludedIds";
        public static T WithExcludedId<T>(this T query, string id) where T : IRepositoryQuery {
            return query.AddSetOptionValue(ExcludedIdsKey, id);
        }

        public static T WithExcludedIds<T>(this T query, params string[] ids) where T : IRepositoryQuery {
            return query.AddSetOptionValue(ExcludedIdsKey, ids);
        }

        public static T WithExcludedIds<T>(this T query, IEnumerable<string> ids) where T : IRepositoryQuery {
            return query.AddSetOptionValue(ExcludedIdsKey, ids);
        }
    }
}

namespace Foundatio.Repositories.Queries {
    public static class ReadIdentityQueryExtensions {
        public static bool HasIds<T>(this T options) where T : IRepositoryQuery {
            return options.SafeHasOption(IdentityQueryExtensions.IdsKey);
        }

        public static ISet<string> GetIds<T>(this T options) where T : IRepositoryQuery {
            return options.SafeGetOption<ISet<string>>(IdentityQueryExtensions.IdsKey, new HashSet<string>());
        }

        public static bool HasExcludedIds<T>(this T options) where T : IRepositoryQuery {
            return options.SafeHasOption(IdentityQueryExtensions.ExcludedIdsKey);
        }

        public static ISet<string> GetExcludedIds<T>(this T options) where T : IRepositoryQuery {
            return options.SafeGetOption<ISet<string>>(IdentityQueryExtensions.ExcludedIdsKey, new HashSet<string>());
        }
    }
}
