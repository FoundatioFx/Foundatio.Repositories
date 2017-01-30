using System;

namespace Foundatio.Repositories.Options {
    public interface INotificationsOption : ICommandOptions {
        bool SendNotifications { get; set; }
    }

    public static class NotificationOptionsExtensions {
        public static T DisableNotifications<T>(this T options) where T : INotificationsOption {
            options.SendNotifications = false;
            return options;
        }

        public static T SendNotifications<T>(this T options, bool sendNotifications = true) where T : INotificationsOption {
            options.SendNotifications = sendNotifications;
            return options;
        }

        public static bool ShouldSendNotifications<T>(this T options) where T : ICommandOptions {
            var notificationOption = options as INotificationsOption;
            if (notificationOption == null)
                return false;

            return notificationOption.SendNotifications;
        }
    }
}
