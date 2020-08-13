using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories.Extensions {
    public static class DictionaryExtensions {
        public static string GetString(this IEnumerable<KeyValuePair<string, object>> data, string name) {
            return data.GetString(name, String.Empty);
        }

        public static string GetString(this IEnumerable<KeyValuePair<string, object>> data, string name, string @default) {
            object value = null;
            if (data is IDictionary<string, object> dictionary) {
                if (!dictionary.TryGetValue(name, out value))
                    return @default;
            } else if (data is IReadOnlyDictionary<string, object> readOnlyDictionary) {
                if (!readOnlyDictionary.TryGetValue(name, out value))
                    return @default;
            } else {
                var d = data.ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
                if (!d.TryGetValue(name, out value))
                    return @default;
            }

            if (value is string s)
                return s;

            return String.Empty;
        }

        public static IDictionary<string, object> ToData<T>(this IEnumerable<KeyValuePair<string, object>> dictionary) where T: IAggregate {
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

        public static IReadOnlyDictionary<string, object> ToReadOnlyData<T>(this IEnumerable<KeyValuePair<string, object>> dictionary) where T : IAggregate {
            return new ReadOnlyDictionary<string, object>(dictionary.ToData<T>());
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
