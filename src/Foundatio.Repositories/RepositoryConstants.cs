using System;

namespace Foundatio.Repositories {
    public static class RepositoryConstants {
        public static TimeSpan DEFAULT_CACHE_EXPIRATION_TIMESPAN = TimeSpan.FromSeconds(DEFAULT_CACHE_EXPIRATION_SECONDS);
        public const int DEFAULT_CACHE_EXPIRATION_SECONDS = 60 * 5;
        public const int DEFAULT_LIMIT = 10;
        public const int MAX_LIMIT = 1000;
    }
}
