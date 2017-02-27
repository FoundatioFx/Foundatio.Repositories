namespace Foundatio.Repositories {
    public static class SetNotificationOptionsExtensions {
        internal const string SendNotificationsKey = "@SendNotifications";

        public static T DisableNotifications<T>(this T options) where T : ICommandOptions {
            return options.BuildOption(SendNotificationsKey, false);
        }

        public static T SendNotifications<T>(this T options, bool sendNotifications = true) where T : ICommandOptions {
            return options.BuildOption(SendNotificationsKey, sendNotifications);
        }
    }
}

namespace Foundatio.Repositories.Options {
    public static class ReadNotificationOptionsExtensions {
        public static bool ShouldSendNotifications<T>(this T options) where T : ICommandOptions {
            return options.SafeGetOption(SetNotificationOptionsExtensions.SendNotificationsKey, true);
        }
    }
}
