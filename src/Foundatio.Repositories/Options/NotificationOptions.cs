namespace Foundatio.Repositories {
    public static class SetNotificationOptionsExtensions {
        internal const string SendNotificationsKey = "@SendNotifications";

        public static T DisableNotifications<T>(this T options) where T : ICommandOptions {
            options.SetOption(SendNotificationsKey, false);
            return options;
        }

        public static T SendNotifications<T>(this T options, bool sendNotifications = true) where T : ICommandOptions {
            options.SetOption(SendNotificationsKey, sendNotifications);
            return options;
        }
    }
}

namespace Foundatio.Repositories.Options {
    public static class ReadNotificationOptionsExtensions {
        public static bool ShouldSendNotifications<T>(this T options) where T : ICommandOptions {
            return options.GetOption(SetNotificationOptionsExtensions.SendNotificationsKey, true);
        }
    }
}
