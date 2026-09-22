from pathlib import Path
import sys
root=Path(sys.argv[1]);mode=sys.argv[2]
p=root/'src/Foundatio.Repositories.Elasticsearch/Repositories/ReindexLeaseGuard.cs'
p.write_text('''using System;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.Lock;
using Foundatio.Repositories.Extensions;

namespace Foundatio.Repositories.Elasticsearch;

/// <summary>Renews a caller-owned lease independently of progress and cancels work when renewal is unconfirmed.</summary>
internal sealed class ReindexLeaseGuard : ILock
{
    private readonly ILock _lease;
    private readonly TimeProvider _timeProvider;
    private readonly CancellationTokenSource _stop = new();
    private readonly CancellationTokenSource _lost = new();
    private readonly SemaphoreSlim _renewal = new(1, 1);
    private readonly Task _heartbeat;
    private Exception? _failure;
    private int _disposed;

    public ReindexLeaseGuard(ILock lease, TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(lease);
        ArgumentNullException.ThrowIfNull(timeProvider);
        _lease = lease;
        _timeProvider = timeProvider;
        _heartbeat = RunHeartbeatAsync();
    }

    public CancellationToken LostToken => _lost.Token;
    public Exception? Failure => Volatile.Read(ref _failure);
    public string LockId => _lease.LockId;
    public string Resource => _lease.Resource;
    public DateTime AcquiredTimeUtc => _lease.AcquiredTimeUtc;
    public TimeSpan TimeWaitedForLock => _lease.TimeWaitedForLock;
    public int RenewalCount => _lease.RenewalCount;

    public async Task RenewAsync(TimeSpan? timeUntilExpires = null)
    {
        if (Failure is { } failure)
            throw new LockException($"Migration lease '{Resource}' is no longer confirmed: {failure.Message}");
        await _renewal.WaitAsync(_stop.Token).AnyContext();
        try
        {
            if (Failure is { } previous)
                throw new LockException($"Migration lease '{Resource}' is no longer confirmed: {previous.Message}");
            await _lease.RenewAsync(timeUntilExpires).WaitAsync(TimeSpan.FromSeconds(30), _timeProvider, _stop.Token).AnyContext();
        }
        catch (OperationCanceledException) when (_stop.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            LoseOwnership(ex);
            throw;
        }
        finally
        {
            _renewal.Release();
        }
    }

    private async Task RunHeartbeatAsync()
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(30), _timeProvider);
        try
        {
            while (await timer.WaitForNextTickAsync(_stop.Token).AnyContext())
                await RenewAsync().AnyContext();
        }
        catch (OperationCanceledException) when (_stop.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            LoseOwnership(ex);
        }
    }

    private void LoseOwnership(Exception exception)
    {
        if (Interlocked.CompareExchange(ref _failure, exception, null) is null)
            _lost.Cancel();
    }

    public async Task ReleaseAsync()
    {
        await DisposeAsync().AnyContext();
        await _lease.ReleaseAsync().AnyContext();
    }

    // The surrounding acquisition scope still owns the underlying lease. Stop renewal before it releases.
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) is not 0)
            return;
        await _stop.CancelAsync().AnyContext();
        await _heartbeat.AnyContext();
        _stop.Dispose();
        _lost.Dispose();
    }
}
''')
p=root/'tests/Foundatio.Repositories.Elasticsearch.Tests/ReindexLeaseGuardTests.cs'
p.write_text('''using System;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.Lock;
using Microsoft.Extensions.Time.Testing;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class ReindexLeaseGuardTests
{
    [Fact]
    public async Task HeartbeatRenewsWithoutProgressCallbacks()
    {
        var time = new FakeTimeProvider();
        var lease = new ProbeLease();
        await using var guard = new ReindexLeaseGuard(lease, time);
        time.Advance(TimeSpan.FromSeconds(31));
        await lease.Renewed.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.Equal(1, lease.RenewalCount);
        Assert.False(guard.LostToken.IsCancellationRequested);
        Assert.Null(guard.Failure);
    }

    [Fact]
    public async Task HeartbeatLossCancelsWorkAndNeverReacquires()
    {
        var time = new FakeTimeProvider();
        var lease = new ProbeLease { Fail = true };
        await using var guard = new ReindexLeaseGuard(lease, time);
        var cancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var registration = guard.LostToken.Register(() => cancelled.TrySetResult());
        time.Advance(TimeSpan.FromSeconds(31));
        await cancelled.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.NotNull(guard.Failure);
        await Assert.ThrowsAsync<LockException>(() => guard.RenewAsync());
        Assert.Equal(1, lease.Attempts);
        Assert.Equal(0, lease.Releases);
    }

    [Fact]
    public async Task DisposingGuardStopsHeartbeatWithoutReleasingCallerOwnedLease()
    {
        var time = new FakeTimeProvider();
        var lease = new ProbeLease();
        var guard = new ReindexLeaseGuard(lease, time);
        await guard.DisposeAsync();
        time.Advance(TimeSpan.FromMinutes(2));
        Assert.Equal(0, lease.Attempts);
        Assert.Equal(0, lease.Releases);
    }

    private sealed class ProbeLease : ILock
    {
        public bool Fail { get; init; }
        public TaskCompletionSource Renewed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int Attempts { get; private set; }
        public int Releases { get; private set; }
        public string LockId => "owner";
        public string Resource => "migration";
        public DateTime AcquiredTimeUtc => DateTime.UtcNow;
        public TimeSpan TimeWaitedForLock => TimeSpan.Zero;
        public int RenewalCount { get; private set; }
        public Task RenewAsync(TimeSpan? timeUntilExpires = null)
        {
            Attempts++;
            if (Fail)
                throw new LockException("lease lost");
            RenewalCount++;
            Renewed.TrySetResult();
            return Task.CompletedTask;
        }
        public Task ReleaseAsync()
        {
            Releases++;
            return Task.CompletedTask;
        }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
''')
if mode=='307':
 p=root/'src/Foundatio.Repositories.Elasticsearch/Configuration/ElasticIndexCompatibilityUpgrader.cs';s=p.read_text()
 old='        string sourceIndex = compatibility.Name;';assert s.count(old)==1
 s=s.replace(old,'''        await using var leaseGuard = new ReindexLeaseGuard(reindexLock, _timeProvider);
        using var guardedCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, leaseGuard.LostToken);
        cancellationToken = guardedCancellation.Token;

'''+old)
 s=s.replace('await reindexLock.RenewAsync().WaitAsync(TimeSpan.FromSeconds(30)).AnyContext();','await leaseGuard.RenewAsync().AnyContext();')
 s=s.replace('if (leaseOwnershipUncertain)', 'if (leaseOwnershipUncertain || leaseGuard.Failure is not null)')
 s=s.replace('using var recoveryCancellation = new CancellationTokenSource(TimeSpan.FromSeconds(30));', 'using var recoveryCancellation = CancellationTokenSource.CreateLinkedTokenSource(leaseGuard.LostToken);\n            recoveryCancellation.CancelAfter(TimeSpan.FromSeconds(30));')
 p.write_text(s)
elif mode=='327':
 p=root/'src/Foundatio.Repositories.Elasticsearch/Configuration/VersionedIndex.cs';s=p.read_text()
 old='''        public ReindexLease(ILock reindexLock, int currentVersion)
        {
            Lock = reindexLock;
            CurrentVersion = currentVersion;
        }

        public ILock Lock { get; }
        public int CurrentVersion { get; }

        public ValueTask DisposeAsync() => Lock.DisposeAsync();'''
 new='''        private readonly ILock _lease;
        private readonly ReindexLeaseGuard _guard;

        public ReindexLease(ILock reindexLock, int currentVersion) : this(reindexLock, currentVersion, TimeProvider.System) { }

        internal ReindexLease(ILock reindexLock, int currentVersion, TimeProvider timeProvider)
        {
            _lease = reindexLock;
            _guard = new ReindexLeaseGuard(reindexLock, timeProvider);
            CurrentVersion = currentVersion;
        }

        public ILock Lock => _guard;
        public CancellationToken LostToken => _guard.LostToken;
        public int CurrentVersion { get; }

        public async ValueTask DisposeAsync()
        {
            await _guard.DisposeAsync().AnyContext();
            await _lease.DisposeAsync().AnyContext();
        }'''
 assert s.count(old)==1;s=s.replace(old,new)
 s=s.replace('return new ReindexLease(reindexLock, currentVersion);', 'return new ReindexLease(reindexLock, currentVersion, Configuration.TimeProvider);')
 marker='''        if (lease is null)
            return;
'''
 assert s.count(marker)==1;s=s.replace(marker,marker+'''        using var guardedCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, lease.LostToken);
        cancellationToken = guardedCancellation.Token;
''')
 s=s.replace('''    /// Wraps the caller's progress callback so the reindex lock is renewed on every progress report, which
    /// is what keeps a migration longer than the lock's TTL from having the lock expire underneath it.''','''    /// Checks lease ownership at progress boundaries in addition to independent bounded heartbeat renewal.''')
 p.write_text(s)
 p=root/'src/Foundatio.Repositories.Elasticsearch/Configuration/DailyIndex.cs';s=p.read_text();assert s.count(marker)==1
 s=s.replace(marker,marker+'''        using var guardedCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, lease.LostToken);
        cancellationToken = guardedCancellation.Token;
''');p.write_text(s)
 p=root/'src/Foundatio.Repositories.Elasticsearch/Jobs/ReindexWorkItemHandler.cs';s=p.read_text()
 old='        var disposition = await GetRedeliveryDispositionAsync(workItem, context.CancellationToken).AnyContext();';assert s.count(old)==1
 s=s.replace(old,'''        await using var guard = context.WorkItemLock is null ? null : new ReindexLeaseGuard(context.WorkItemLock, TimeProvider.System);
        using var guardedCancellation = CancellationTokenSource.CreateLinkedTokenSource(context.CancellationToken, guard?.LostToken ?? CancellationToken.None);
        var cancellationToken = guardedCancellation.Token;
        var disposition = await GetRedeliveryDispositionAsync(workItem, cancellationToken).AnyContext();''')
 s=s.replace('context.ReportProgressAsync, context.CancellationToken', 'context.ReportProgressAsync, cancellationToken');p.write_text(s)
else:raise ValueError(mode)
for name in ['docs/guide/index-management.md','.agents/skills/foundatio-repositories/references/index-lifecycle.md']:
 p=root/name
 with p.open('a') as f:
  f.write('\nMigration leases now renew on an independent 30-second heartbeat with bounded renewal attempts, not solely on progress callbacks. Renewal failure cancels the linked operation and forbids further guarded work. This requires providers to report loss correctly (Foundatio #573); it is not a fencing token and cannot revoke already-dispatched Elasticsearch requests. Underlying lease release remains owned by the acquisition scope.\n')
