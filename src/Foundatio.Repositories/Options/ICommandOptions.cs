using System;

namespace Foundatio.Repositories.Options {
    /// <summary>
    /// Marker interface to enforce that parameters are command options
    /// </summary>
    public interface ICommandOptions { }

    public class CommandOptions : INotificationsOption, IPagingOptions, ICacheOptions {
        public bool SendNotifications { get; set; }
        public int? Limit { get; set; }
        public int? Page { get; set; }
        public bool UseAutoCache { get; set; }
        public string CacheKey { get; set; }
        public TimeSpan? ExpiresIn { get; set; }
        public DateTime? ExpiresAtUtc { get; set; }
    }
}
