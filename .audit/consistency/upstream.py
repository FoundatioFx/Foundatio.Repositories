from pathlib import Path
import sys

root = Path(sys.argv[1])
mode = sys.argv[2]
if mode == 'tests':
    path = root / 'tests/Foundatio.Tests/Locks/CacheLockRenewalTests.cs'
    path.write_text('''using System;
using System.Threading.Tasks;
using Foundatio.Caching;
using Foundatio.Lock;
using Microsoft.Extensions.Time.Testing;
using Xunit;

namespace Foundatio.Tests.Locks;

public sealed class CacheLockRenewalTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RenewAsync_WhenLeaseExpires_RejectsStaleOwner(bool acquiredByAnotherOwner)
    {
        var time = new FakeTimeProvider();
        using var cache = new InMemoryCacheClient(o => o.TimeProvider(time));
        var provider = new CacheLockProvider(cache, null, time);
        await using var stale = await provider.AcquireAsync("migration", TimeSpan.FromMinutes(1), cancellationToken: TestContext.Current.CancellationToken);
        time.Advance(TimeSpan.FromMinutes(2));
        await using var replacement = acquiredByAnotherOwner
            ? await provider.AcquireAsync("migration", TimeSpan.FromMinutes(10), cancellationToken: TestContext.Current.CancellationToken)
            : null;

        await Assert.ThrowsAsync<LockException>(() => stale.RenewAsync());
        Assert.Equal(0, stale.RenewalCount);
        if (replacement is not null)
        {
            await replacement.RenewAsync();
            await stale.ReleaseAsync();
            Assert.True(await provider.IsLockedAsync("migration"));
        }
        else
            Assert.False(await provider.IsLockedAsync("migration"));
    }

    [Fact]
    public async Task RenewAsync_AfterRelease_DoesNotRecreateLease()
    {
        using var cache = new InMemoryCacheClient();
        var provider = new CacheLockProvider(cache, null);
        await using var lease = await provider.AcquireAsync("migration", cancellationToken: TestContext.Current.CancellationToken);
        await lease.ReleaseAsync();
        await Assert.ThrowsAsync<LockException>(() => lease.RenewAsync());
        Assert.False(await provider.IsLockedAsync("migration"));
        Assert.Equal(0, lease.RenewalCount);
    }

    [Fact]
    public async Task RenewAsync_WithCurrentOwner_ExtendsLease()
    {
        var time = new FakeTimeProvider();
        using var cache = new InMemoryCacheClient(o => o.TimeProvider(time));
        var provider = new CacheLockProvider(cache, null, time);
        await using var lease = await provider.AcquireAsync("migration", TimeSpan.FromMinutes(1), cancellationToken: TestContext.Current.CancellationToken);
        time.Advance(TimeSpan.FromSeconds(30));
        await lease.RenewAsync(TimeSpan.FromMinutes(3));
        time.Advance(TimeSpan.FromMinutes(1));
        Assert.True(await provider.IsLockedAsync("migration"));
        Assert.Equal(1, lease.RenewalCount);
    }
}
''')
elif mode == 'fix':
    path = root / 'src/Foundatio/Lock/CacheLockProvider.cs'
    text = path.read_text(encoding='utf-8-sig')
    old = '    public Task RenewAsync(string resource, string lockId, TimeSpan? timeUntilExpires = null)'
    assert text.count(old) == 1
    text = text.replace(old, '''    /// <summary>Extends the lease only while it is still owned by the supplied lock ID.</summary>
    /// <exception cref="LockException">The lease expired, was released, or belongs to another owner.</exception>
    public async Task RenewAsync(string resource, string lockId, TimeSpan? timeUntilExpires = null)''')
    old = '        return _resiliencePolicy.ExecuteAsync(async _ => await _cacheClient.ReplaceIfEqualAsync(resource, lockId, lockId, timeUntilExpires.Value)).AsTask();'
    assert text.count(old) == 1
    text = text.replace(old, '''        bool renewed = await _resiliencePolicy.ExecuteAsync(async _ =>
            await _cacheClient.ReplaceIfEqualAsync(resource, lockId, lockId, timeUntilExpires.Value).AnyContext()).AnyContext();
        if (!renewed)
            throw new LockException($"Cannot renew lock '{resource}': the lease expired, was released, or is owned by another caller.");''')
    path.write_text(text)
    path = root / 'docs/guide/distributed-locks.md'
    if not path.exists():
        path = root / 'docs/guide/locks.md'
    assert path.exists(), path
    with path.open('a') as f:
        f.write('\n\n### Lost lease ownership\n\n`CacheLockProvider.RenewAsync` throws `LockException` when the cached lease no longer matches the original lock ID. An expired or released lease is not recreated, and another owner\'s lease is not extended or removed. Stop protected work after a renewal failure; do not interpret a failed compare-and-renew as successful renewal. This is an intentional behavior correction for callers that previously continued after losing ownership.\n\nRenewal is not a fencing token: it cannot revoke requests already submitted to an external service. Long-running workflows still need bounded independent renewal, durable operation identity, and recovery rules that retain uncertain work rather than authorizing destructive cleanup.\n')
else:
    raise ValueError(mode)
