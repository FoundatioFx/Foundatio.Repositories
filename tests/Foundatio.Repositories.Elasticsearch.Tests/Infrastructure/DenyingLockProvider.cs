using System;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.Lock;

namespace Foundatio.Repositories.Elasticsearch.Tests.Infrastructure;

/// <summary>Lock provider that always denies acquisition, as happens on lock timeout.</summary>
/// <remarks>
/// Returns <c>null</c> from a signature the interface declares as non-nullable (<c>Task&lt;ILock&gt;</c>).
/// That mismatch is precisely why the production null check was missing: the compiler never warned that
/// the result could be null.
/// </remarks>
public sealed class DenyingLockProvider : ILockProvider
{
    public Task<ILock> AcquireAsync(string resource, TimeSpan? timeUntilExpires = null, bool releaseOnDispose = true, CancellationToken cancellationToken = default)
        => Task.FromResult<ILock>(null!);

    public Task<ILock?> TryAcquireAsync(string resource, TimeSpan? timeUntilExpires = null, bool releaseOnDispose = true, CancellationToken cancellationToken = default)
        => Task.FromResult<ILock?>(null);

    public Task<bool> IsLockedAsync(string resource) => Task.FromResult(true);

    public Task ReleaseAsync(string resource, string lockId) => Task.CompletedTask;

    public Task ReleaseAsync(string resource) => Task.CompletedTask;

    public Task RenewAsync(string resource, string lockId, TimeSpan? timeUntilExpires = null) => Task.CompletedTask;
}
