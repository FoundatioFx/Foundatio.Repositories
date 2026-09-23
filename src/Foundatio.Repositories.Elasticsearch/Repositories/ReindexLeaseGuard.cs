using System;
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
