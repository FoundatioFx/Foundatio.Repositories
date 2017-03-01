using System;
using Foundatio.Repositories.Options;
using Foundatio.Utility;

namespace Foundatio.Repositories {
    public static class SetCacheOptionsExtensions {
        internal const string AutoCacheKey = "@AutoCache";

        public static T AutoCache<T>(this T options, bool enabled = true) where T : ICommandOptions {
            return options.BuildOption(AutoCacheKey, enabled);
        }

        internal const string CacheKeyKey = "@CacheKey";
        public static T CacheKey<T>(this T options, string cacheKey) where T : ICommandOptions {
            return options.BuildOption(CacheKeyKey, cacheKey);
        }

        internal const string CacheExpiresInKey = "@CacheExpiresIn";
        public static T CacheExpiresIn<T>(this T options, TimeSpan? expiresIn) where T : ICommandOptions {
            if (expiresIn.HasValue)
                return options.BuildOption(CacheExpiresInKey, expiresIn.Value);

            return options;
        }

        public static T CacheExpiresAt<T>(this T options, DateTime? expiresAtUtc) where T : ICommandOptions {
            if (expiresAtUtc.HasValue)
                return options.BuildOption(CacheExpiresInKey, expiresAtUtc.Value.Subtract(SystemClock.UtcNow));

            return options;
        }
    }
}

namespace Foundatio.Repositories.Options {
    public static class ReadCacheOptionsExtensions {
        public static bool ShouldUseCache<T>(this T options) where T : ICommandOptions {
            return options.SafeHasOption(SetCacheOptionsExtensions.CacheKeyKey) || options.SafeGetOption(SetCacheOptionsExtensions.AutoCacheKey, false);
        }

        public static bool HasCacheKey<T>(this T options) where T : ICommandOptions {
            return options.SafeGetOption<bool>(SetCacheOptionsExtensions.CacheKeyKey);
        }

        public static string GetCacheKey<T>(this T options) where T : ICommandOptions {
            return options.SafeGetOption<string>(SetCacheOptionsExtensions.CacheKeyKey, null);
        }

        public static TimeSpan GetExpiresIn<T>(this T options) where T : ICommandOptions {
            return options.SafeGetOption(SetCacheOptionsExtensions.CacheExpiresInKey, RepositoryConstants.DEFAULT_CACHE_EXPIRATION_TIMESPAN);
        }
    }
}

