using System;

namespace Foundatio.Repositories.Queries {
    public interface ISystemFilterQuery {
        object SystemFilter { get; set; }
    }

    public static class SystemFilterQueryExtensions {
        public static T WithSystemFilter<T>(this T query, object filter) where T : ISystemFilterQuery {
            query.SystemFilter = filter;
            return query;
        }
    }
}
