using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories.Elasticsearch.Extensions {
    public static class FindHitExtensions {
        public static string GetIndex<T>(this FindHit<T> hit) {
            return hit.Data.GetString(ElasticDataKeys.Index);
        }

        public static string GetIndexType<T>(this FindHit<T> hit) {
            return hit.Data.GetString(ElasticDataKeys.IndexType);
        }
    }

    public static class ElasticDataKeys {
        public const string Index = "index";
        public const string IndexType = "indextype";
        public const string ScrollId = "scrollid";
    }
}
