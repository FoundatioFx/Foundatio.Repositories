using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories.Elasticsearch.Extensions {
    public static class FindResultsExtensions {
        public static string GetScrollId(this IHaveData results) {
            return results.Data.GetString(ElasticDataKeys.ScrollId);
        }
    }
}
