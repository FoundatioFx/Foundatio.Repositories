using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace Foundatio.Repositories.Extensions {
    internal static class TaskHelper {
        [DebuggerStepThrough]
        public static ConfiguredTaskAwaitable<TResult> AnyContext<TResult>(this Task<TResult> task) {
            return task.ConfigureAwait(continueOnCapturedContext: false);
        }

        [DebuggerStepThrough]
        public static ConfiguredTaskAwaitable AnyContext(this Task task) {
            return task.ConfigureAwait(continueOnCapturedContext: false);
        }
    }
}
