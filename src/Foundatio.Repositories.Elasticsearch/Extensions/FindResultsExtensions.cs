using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories.Elasticsearch.Extensions {
    public static class FindResultsExtensions {
        public static string GetScrollId(this IHaveData results) {
            return results.Data.GetString(ElasticDataKeys.ScrollId, null);
        }

        public static string GetAsyncSearchId(this IHaveData results) {
            return results.Data.GetString(ElasticDataKeys.AsyncSearchId, null);
        }

        public static bool GetIsPending(this IHaveData results) {
            return results.Data.GetBoolean(ElasticDataKeys.IsPending);
        }

        public static bool GetIsRunning(this IHaveData results) {
            return results.Data.GetBoolean(ElasticDataKeys.IsRunning);
        }
    }
}
