using System;
using System.Collections.Generic;

namespace Foundatio.Repositories.Extensions {
    public static class DictionaryExtensions {
        public static string GetString(this IReadOnlyDictionary<string, object> data, string name) {
            return data.GetString(name, String.Empty);
        }

        public static string GetString(this IReadOnlyDictionary<string, object> data, string name, string @default) {
            object value;

            if (!data.TryGetValue(name, out value))
                return @default;

            if (value is string)
                return (string)value;

            return String.Empty;
        }
    }
}
