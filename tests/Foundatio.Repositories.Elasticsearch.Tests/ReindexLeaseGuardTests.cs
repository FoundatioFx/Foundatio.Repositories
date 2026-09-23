using System;
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
