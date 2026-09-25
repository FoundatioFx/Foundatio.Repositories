using System;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.Lock;

namespace Foundatio.Repositories.Elasticsearch.Tests.Infrastructure;

/// <summary>Lock provider that always denies acquisition by throwing, as the real providers do on timeout.</summary>
/// <remarks>
/// This is the production-accurate denial: <c>CacheLockProvider</c>'s acquire-with-timeout overload throws
/// <see cref="LockAcquisitionTimeoutException"/> rather than returning null. A fake that returns null instead
/// would let a handled-null implementation pass while the real contention path threw straight through the
/// migration, so contention must be exercised the way it actually happens.
/// </remarks>
public sealed class ThrowingLockProvider : ILockProvider
{
    public Task<ILock> AcquireAsync(string resource, TimeSpan? timeUntilExpires = null, bool releaseOnDispose = true, CancellationToken cancellationToken = default)
        => throw new LockAcquisitionTimeoutException(resource);

    public Task<ILock?> TryAcquireAsync(string resource, TimeSpan? timeUntilExpires = null, bool releaseOnDispose = true, CancellationToken cancellationToken = default)
        => Task.FromResult<ILock?>(null);

    public Task<bool> IsLockedAsync(string resource) => Task.FromResult(true);

    public Task ReleaseAsync(string resource, string lockId) => Task.CompletedTask;

    public Task ReleaseAsync(string resource) => Task.CompletedTask;

    public Task RenewAsync(string resource, string lockId, TimeSpan? timeUntilExpires = null) => Task.CompletedTask;
}
