using System;
using System.Threading;
using System.Threading.Tasks;

namespace Foundatio.Repositories.Extensions;

internal static class TimeProviderExtensions
{
    public static async Task SafeDelay(this TimeProvider timeProvider, TimeSpan delay, CancellationToken cancellationToken = default)
    {
        try
        {
            await Task.Delay(delay, timeProvider, cancellationToken);
        }
        catch (OperationCanceledException)
        {
            // Intentionally swallowed: callers use SafeDelay to avoid surfacing cancellation as an error
        }
    }
}
