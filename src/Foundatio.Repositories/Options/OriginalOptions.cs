using System.Collections.Generic;
using Foundatio.Repositories.Options;

namespace Foundatio.Repositories {
    public static class OriginalOptionsExtensions {
        internal const string OriginalsEnabledKey = "@OriginalsEnabled";
        public static T Originals<T>(this T options, bool enabled = true) where T : ICommandOptions {
            options.Values.Set(OriginalsEnabledKey, enabled);
            return options;
        }

        internal const string OriginalsValuesKey = "@OriginalsValues";
        public static TOptions AddOriginals<TOptions, TValue>(this TOptions options, TValue value) where TOptions : ICommandOptions {
            return options.AddCollectionOptionValue(OriginalsValuesKey, value);
        }

        public static TOptions AddOriginals<TOptions, TValue>(this TOptions options, IEnumerable<TValue> values) where TOptions : ICommandOptions {
            return options.AddCollectionOptionValue(OriginalsValuesKey, values);
        }
    }
}

namespace Foundatio.Repositories.Options {
    public static class ReadOriginalOptionsExtensions {
        public static ICollection<T> GetOriginals<T>(this ICommandOptions options) {
            return options.SafeGetCollection<T>(OriginalOptionsExtensions.OriginalsValuesKey);
        }

        public static bool GetOriginalsEnabled(this ICommandOptions options, bool defaultEnabled = true) {
            return options.SafeGetOption(OriginalOptionsExtensions.OriginalsEnabledKey, defaultEnabled);
        }
    }
}

