using System;
using System.Collections.Generic;
using System.Linq;
using Foundatio.Repositories.Extensions;

namespace Foundatio.Repositories.Queries {
    public interface IIdentityQuery : IRepositoryQuery {
        ISet<string> Ids { get; }
        ISet<string> ExcludedIds { get; }
    }

    public static class IdentityQueryExtensions {
        public static T WithId<T>(this T query, string id) where T : IIdentityQuery {
            query.Ids.Add(id);
            return query;
        }

        public static T WithIds<T>(this T query, params string[] ids) where T : IIdentityQuery {
            query.Ids.AddRange(ids.Distinct());
            return query;
        }

        public static T WithIds<T>(this T query, IEnumerable<string> ids) where T : IIdentityQuery {
            query.Ids.AddRange(ids.Distinct());
            return query;
        }

        public static T WithExcludedId<T>(this T query, string id) where T : IIdentityQuery {
            query.ExcludedIds.Add(id);
            return query;
        }

        public static T WithExcludedIds<T>(this T query, params string[] ids) where T : IIdentityQuery {
            query.ExcludedIds.AddRange(ids.Distinct());
            return query;
        }

        public static T WithExcludedIds<T>(this T query, IEnumerable<string> ids) where T : IIdentityQuery {
            query.ExcludedIds.AddRange(ids.Distinct());
            return query;
        }
    }
}
