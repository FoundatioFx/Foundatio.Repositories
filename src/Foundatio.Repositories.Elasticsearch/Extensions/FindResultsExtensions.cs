using Foundatio.Utility;

namespace Foundatio.Repositories.Elasticsearch.Extensions;

public static class FindResultsExtensions
{
    public static string GetScrollId(this IHaveData results)
    {
        return results.Data.GetString(ElasticDataKeys.ScrollId, null);
    }
}
