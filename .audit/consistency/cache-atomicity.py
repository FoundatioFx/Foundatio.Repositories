from pathlib import Path
import sys,re
root=Path(sys.argv[1]);mode=sys.argv[2]
if mode=='tests':
 p=root/'tests/Foundatio.Tests/Locks/CacheConditionalRaceTests.cs'
 p.write_text('''using System;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.Caching;
using Xunit;

namespace Foundatio.Tests.Locks;

public sealed class CacheConditionalRaceTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ConditionalMutation_DoesNotReportSuccessAfterAnotherOwnerReplacesTheEntry(bool remove)
    {
        using var cache = new InMemoryCacheClient(o => o.CloneValues(false));
        using var release = new ManualResetEventSlim();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var original = new GateValue("original", () =>
        {
            entered.TrySetResult();
            if (!release.Wait(TimeSpan.FromSeconds(10)))
                throw new TimeoutException("Test did not release the paused comparison.");
        });
        var replacementOwner = new GateValue("replacement");
        await cache.SetAsync("lease", original);
        var pending = Task.Run(() => remove
            ? cache.RemoveIfEqualAsync("lease", original)
            : cache.ReplaceIfEqualAsync("lease", new GateValue("renewed"), original), TestContext.Current.CancellationToken);
        try
        {
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
            await cache.SetAsync("lease", replacementOwner);
        }
        finally
        {
            release.Set();
        }
        Assert.False(await pending);
        Assert.Same(replacementOwner, (await cache.GetAsync<GateValue>("lease")).Value);
    }

    private sealed class GateValue(string id, Action? beforeEquals = null) : IEquatable<GateValue>
    {
        public string Id { get; } = id;
        public bool Equals(GateValue? other)
        {
            beforeEquals?.Invoke();
            return other is not null && String.Equals(Id, other.Id, StringComparison.Ordinal);
        }
        public override bool Equals(object? obj) => obj is GateValue other && Equals(other);
        public override int GetHashCode() => StringComparer.Ordinal.GetHashCode(Id);
    }
}
''')
elif mode=='fix':
 p=root/'src/Foundatio/Caching/InMemoryCacheClient.cs';s=p.read_text(encoding='utf-8-sig')
 def replace_method(name,new):
  global s
  pattern=r'^    public async Task<bool> '+name+r'<T>\([^\n]*\)\n    \{.*?^    \}'
  match=re.search(pattern,s,re.M|re.S);assert match,name;s=s[:match.start()]+new+s[match.end():]
 replace_method('RemoveIfEqualAsync','''    public async Task<bool> RemoveIfEqualAsync<T>(string key, T expected)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        bool removed = false;
        while (_memory.TryGetValue(key, out var entry))
        {
            if (entry.IsExpired || !EqualityComparer<T>.Default.Equals(entry.GetValue<T>(), expected))
                break;

            // Remove only this exact entry. A concurrent replacement requires a new value comparison,
            // not a success flag retained from a failed update factory invocation.
            if (!((ICollection<KeyValuePair<string, CacheEntry>>)_memory).Remove(new KeyValuePair<string, CacheEntry>(key, entry)))
                continue;

            UpdateMemorySize(-entry.Size);
            removed = true;
            break;
        }
        await StartMaintenanceAsync().AnyContext();
        _logger.LogTrace("RemoveIfEqualAsync Key: {Key} Expected: {Expected} Success: {Success}", key, expected, removed);
        return removed;
    }''')
 replace_method('ReplaceIfEqualAsync','''    public async Task<bool> ReplaceIfEqualAsync<T>(string key, T value, T expected, TimeSpan? expiresIn = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        if (expiresIn < CacheClientExtensions.MinimumExpiration)
        {
            RemoveExpiredKey(key);
            return false;
        }

        Interlocked.Increment(ref _writes);
        DateTime? expiresAt = expiresIn.HasValue ? _timeProvider.GetUtcNow().UtcDateTime.SafeAdd(expiresIn.Value) : null;
        var replacement = CreateEntry(value, expiresAt);
        if (replacement is null)
            return false;
        bool replaced = false;
        while (_memory.TryGetValue(key, out var entry))
        {
            if (entry.IsExpired || !EqualityComparer<T>.Default.Equals(entry.GetValue<T>(), expected))
                break;

            // Never mutate the comparison entry before CAS: update factories can run more than once,
            // and a stale invocation must neither change an entry nor report another owner's success.
            if (!_memory.TryUpdate(key, replacement, entry))
                continue;

            UpdateMemorySize(replacement.Size - entry.Size);
            replaced = true;
            break;
        }
        await StartMaintenanceAsync().AnyContext();
        _logger.LogTrace("ReplaceIfEqualAsync Key: {Key} Expected: {Expected} Success: {Success}", key, expected, replaced);
        return replaced;
    }''')
 p.write_text(s)
else:raise ValueError(mode)
