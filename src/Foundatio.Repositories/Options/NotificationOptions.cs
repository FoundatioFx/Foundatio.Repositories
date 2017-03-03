using Foundatio.Repositories.Options;

namespace Foundatio.Repositories {
    public static class SetNotificationOptionsExtensions {
        internal const string NotificationsKey = "@Notifications";

        public static T Notifications<T>(this T options, bool sendNotifications = true) where T : ICommandOptions {
            return options.BuildOption(NotificationsKey, sendNotifications);
        }
    }
}

namespace Foundatio.Repositories.Options {
    public static class ReadNotificationOptionsExtensions {
        public static bool ShouldNotify(this ICommandOptions options) {
            return options.SafeGetOption(SetNotificationOptionsExtensions.NotificationsKey, true);
        }
    }
}
