using System;
using Foundatio.Repositories.Queries;

namespace Foundatio.Repositories.Elasticsearch.Queries {
    public interface ITypeQuery: IRepositoryQuery {
        string Type { get; set; }
    }

    public static class TypeQueryExtensions {
        public static T WithType<T>(this T query, string type) where T : ITypeQuery {
            query.Type = type;
            return query;
        }
    }
}
