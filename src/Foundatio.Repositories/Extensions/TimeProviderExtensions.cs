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
            await timeProvider.Delay(delay, cancellationToken);
        }
        catch (OperationCanceledException)
        {
        }
    }
}
