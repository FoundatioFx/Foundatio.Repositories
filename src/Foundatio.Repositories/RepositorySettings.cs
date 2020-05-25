using System;

namespace Foundatio.Repositories {
    public static class RepositorySettings {
        public static TimeSpan DefaultCacheExpiration { get; set; } = TimeSpan.FromSeconds(60 * 5);
        public static int DefaultLimit { get; set; } = 10;
        public static int MaxLimit { get; set; } = 10000;
    }
}
