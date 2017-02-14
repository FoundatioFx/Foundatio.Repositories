using System;
using Foundatio.Utility;

namespace Foundatio.Repositories {
    public static class SetCacheOptionsExtensions {
        internal const string EnableCacheKey = "@EnableCache";

        public static T EnableCache<T>(this T options, bool enableCache) where T : ICommandOptions {
            options.SetOption(EnableCacheKey, enableCache);
            return options;
        }

        public static T EnableCache<T>(this T options, bool enableCache, DateTime? expiresAtUtc = null) where T : ICommandOptions {
            options.SetOption(EnableCacheKey, enableCache);
            if (expiresAtUtc.HasValue)
                options.SetOption(CacheExpiresInKey, expiresAtUtc.Value.Subtract(SystemClock.UtcNow));

            return options;
        }

        public static T EnableCache<T>(this T options, bool enableCache, TimeSpan? expiresIn = null) where T : ICommandOptions {
            options.SetOption(EnableCacheKey, enableCache);
            if (expiresIn.HasValue)
                options.SetOption(CacheExpiresInKey, expiresIn.Value);

            return options;
        }

        internal const string CacheKey = "@CacheKey";
        internal const string CacheExpiresInKey = "@CacheExpiresIn";
        public static T UseCache<T>(this T options, string cacheKey, DateTime? expiresAtUtc = null) where T : ICommandOptions {
            options.SetOption(CacheKey, cacheKey);
            if (expiresAtUtc.HasValue)
                options.SetOption(CacheExpiresInKey, expiresAtUtc.Value.Subtract(SystemClock.UtcNow));

            return options;
        }

        public static T UseCache<T>(this T options, string cacheKey, TimeSpan? expiresIn = null) where T : ICommandOptions {
            options.SetOption(CacheKey, cacheKey);
            if (expiresIn.HasValue)
                options.SetOption(CacheExpiresInKey, expiresIn.Value);

            return options;
        }
    }
}

namespace Foundatio.Repositories.Options {
    public static class ReadCacheOptionsExtensions {
        public static bool ShouldUseCache<T>(this T options) where T : ICommandOptions {
            if (options.HasOption(SetCacheOptionsExtensions.EnableCacheKey))
                return options.GetOption(SetCacheOptionsExtensions.EnableCacheKey, false);

            return options.HasOption(SetCacheOptionsExtensions.CacheKey);
        }

        public static TimeSpan GetExpiresIn<T>(this T options) where T : ICommandOptions {
            return options.GetOption(SetCacheOptionsExtensions.CacheExpiresInKey, RepositoryConstants.DEFAULT_CACHE_EXPIRATION_TIMESPAN);
        }
    }
}

