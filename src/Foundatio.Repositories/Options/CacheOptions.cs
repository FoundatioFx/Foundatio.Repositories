using System;
using Foundatio.Utility;

namespace Foundatio.Repositories.Options {
    public interface ICacheOptions : ICommandOptions {
        bool UseAutoCache { get; set; }
        string CacheKey { get; set; }
        TimeSpan? ExpiresIn { get; set; }
        DateTime? ExpiresAtUtc { get; set; }
    }

    public static class CacheOptionsExtensions {
        public static T UseAutoCache<T>(this T options, bool useAutoCache = true) where T : ICacheOptions {
            options.UseAutoCache = useAutoCache;
            return options;
        }

        public static T WithCacheKey<T>(this T options, string cacheKey) where T : ICacheOptions {
            options.CacheKey = cacheKey;
            return options;
        }

        public static T WithExpiresAt<T>(this T options, DateTime? expiresAtUtc) where T : ICacheOptions {
            options.ExpiresAtUtc = expiresAtUtc;
            return options;
        }

        public static T WithExpiresIn<T>(this T options, TimeSpan? expiresIn) where T : ICacheOptions {
            options.ExpiresIn = expiresIn;
            return options;
        }

        public static bool ShouldUseCache<T>(this T options) where T : ICommandOptions {
            var cacheOptions = options as ICacheOptions;
            if (cacheOptions == null)
                return false;

            return !String.IsNullOrEmpty(cacheOptions.CacheKey);
        }

        public static TimeSpan GetExpiresIn<T>(this T options) where T : ICommandOptions {
            var cacheOptions = options as ICacheOptions;
            if (cacheOptions == null)
                return RepositoryConstants.DEFAULT_CACHE_EXPIRATION_TIMESPAN;

            if (cacheOptions.ExpiresIn.HasValue)
                return cacheOptions.ExpiresIn.Value;

            return cacheOptions.ExpiresAtUtc.Value.Subtract(SystemClock.UtcNow);
        }
    }
}
