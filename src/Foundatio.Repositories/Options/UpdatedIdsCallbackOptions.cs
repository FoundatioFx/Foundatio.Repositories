using System;
using System.Collections.Generic;
using Foundatio.Repositories.Options;

namespace Foundatio.Repositories {
    public static class UpdatedIdsCallbackOptionsExtensions {
        internal const string UpdatedIdsCallbackKey = "@UpdatedIdsCallback";

        public static T UpdateCallback<T>(this T options, Action<IEnumerable<string>> callback) where T : ICommandOptions {
            return options.BuildOption(UpdatedIdsCallbackKey, callback);
        }
    }
}

namespace Foundatio.Repositories.Options {
    public static class ReadUpdatedIdsCallbackOptionsExtensions {
        public static Action<IEnumerable<string>> GetUpdatedIdsCallback(this ICommandOptions options) {
            return options.SafeGetOption<Action<IEnumerable<string>>>(UpdatedIdsCallbackOptionsExtensions.UpdatedIdsCallbackKey, null);
        }
    }
}
