using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
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

        private static readonly Task _defaultCompleted = FromResult(new AsyncVoid());
        public static Task Completed() {
            return _defaultCompleted;
        }

        public static Task<TResult> FromResult<TResult>(TResult result) {
            var completionSource = new TaskCompletionSource<TResult>();
            completionSource.SetResult(result);
            return completionSource.Task;
        }

        [StructLayout(LayoutKind.Sequential, Size = 1)]
        private struct AsyncVoid { }
    }
}
