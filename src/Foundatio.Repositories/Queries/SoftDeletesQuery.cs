using System;

namespace Foundatio.Repositories.Queries {
    public interface ISoftDeletesQuery : IRepositoryQuery {
        SoftDeleteQueryMode? SoftDeleteMode { get; set; }
    }

    public enum SoftDeleteQueryMode {
        ActiveOnly,
        DeletedOnly,
        All
    }

    public static class SoftDeletesQueryExtensions {
        public static T IncludeDeleted<T>(this T query, bool includeDeleted = true) where T : ISoftDeletesQuery {
            query.SoftDeleteMode = includeDeleted ? SoftDeleteQueryMode.All : SoftDeleteQueryMode.ActiveOnly;
            return query;
        }

        public static T IncludeOnlyDeleted<T>(this T query, bool includeDeleted = true) where T : ISoftDeletesQuery {
            query.SoftDeleteMode = SoftDeleteQueryMode.DeletedOnly;
            return query;
        }
    }
}
