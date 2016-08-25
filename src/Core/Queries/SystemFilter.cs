using System;

namespace Foundatio.Repositories.Queries {
    public interface ISystemFilterQuery {
        IRepositoryQuery SystemFilter { get; set; }
    }

    public static class SystemFilterQueryExtensions {
        public static T WithSystemFilter<T>(this T query, IRepositoryQuery filter) where T : ISystemFilterQuery {
            query.SystemFilter = filter;
            return query;
        }
    }
}
