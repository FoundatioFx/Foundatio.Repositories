using System;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories.Elasticsearch.Extensions {
    public static class FindResultsExtensions {
        public static string GetScrollId<T>(this FindResults<T> results) where T : class {
            return results.Data.GetString(ElasticDataKeys.ScrollId);
        }
    }
}
