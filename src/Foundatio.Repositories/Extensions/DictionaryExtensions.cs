using System;
using System.Collections.Generic;
using System.Linq;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories.Extensions {
    public static class DictionaryExtensions {
        public static string GetString(this IReadOnlyDictionary<string, object> data, string name) {
            return data.GetString(name, String.Empty);
        }

        public static string GetString(this IReadOnlyDictionary<string, object> data, string name, string @default) {
            if (!data.TryGetValue(name, out var value))
                return @default;

            if (value is string s)
                return s;

            return String.Empty;
        }

        public static IReadOnlyDictionary<string, object> ToData<T>(this IReadOnlyDictionary<string, object> dictionary) where T: IAggregate {
            var dict = dictionary?
                .Where(kvp => kvp.Key != "@field_type" && kvp.Key != "@timezone")
                .ToDictionary(kvp => kvp.Key, kvp => kvp.Value);

            string type = GetAggregateType(typeof(T));
            if (dict == null && type != null)
                dict = new Dictionary<string, object>();

            if (type != null)
                dict["@type"] = type;

            return dict?.Count > 0 ? dict : null;
        }

        private static string GetAggregateType(Type type) {
            if (type == typeof(BucketAggregate))
                return "bucket";

            if (type == typeof(ExtendedStatsAggregate))
                return "exstats";

            if (type == typeof(ObjectValueAggregate))
                return "ovalue";

            if (type == typeof(PercentilesAggregate))
                return "percentiles";

            if (type == typeof(SingleBucketAggregate))
                return "sbucket";

            if (type == typeof(StatsAggregate))
                return "stats";

            if (type == typeof(TopHitsAggregate))
                return "tophits";

            if (type == typeof(ValueAggregate))
                return "value";

            if (type == typeof(ValueAggregate<DateTime>))
                return "dvalue";

            return null;
        }
    }
}
