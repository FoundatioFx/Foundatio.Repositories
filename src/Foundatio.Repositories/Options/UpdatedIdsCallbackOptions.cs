using System;
using System.Collections.Generic;

namespace Foundatio.Repositories.Options {
    public interface IUpdatedIdsCallbackOptions : ICommandOptions {
        Action<IEnumerable<string>> UpdatedIdsCallback { get; set; }
    }

    public static class UpdatedIdsCallbackOptionsExtensions {
        public static T WithUpdatedIdsCallback<T>(this T options, Action<IEnumerable<string>> callback) where T : IUpdatedIdsCallbackOptions {
            options.UpdatedIdsCallback = callback;
            return options;
        }

        public static Action<IEnumerable<string>> GetUpdatedIdsCallback<T>(this T options) where T : ICommandOptions {
            var updatedIdsOptions = options as IUpdatedIdsCallbackOptions;
            if (updatedIdsOptions == null)
                return null;

            return updatedIdsOptions.UpdatedIdsCallback;
        }
    }
}
