from pathlib import Path
import sys
root = Path(sys.argv[1])
p = root / 'src/Foundatio/Caching/InMemoryCacheClient.cs'
s = p.read_text(encoding='utf-8-sig')
start = s.index('    public async Task<bool> ReplaceIfEqualAsync<T>')
end = s.index('    public async Task<double> IncrementAsync', start)
part = s[start:end]
old = '            var currentValue = existingEntry.GetValue<T>();'
assert part.count(old) == 1
part = part.replace(old, '''            if (existingEntry.IsExpired)
                return existingEntry;

            var currentValue = existingEntry.GetValue<T>();''')
p.write_text(s[:start] + part + s[end:])
p = root / 'tests/Foundatio.Tests/Locks/CacheLockRenewalTests.cs'
s = p.read_text()
insert = '''    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ReplaceIfEqualAsync_DoesNotReviveExpiredEntries(bool explicitlyRemoved)
    {
        var time = new FakeTimeProvider();
        using var cache = new InMemoryCacheClient(o => o.TimeProvider(time));
        await cache.SetAsync("lease", "owner", TimeSpan.FromMinutes(1));
        if (explicitlyRemoved)
            Assert.True(await cache.RemoveIfEqualAsync("lease", "owner"));
        else
            time.Advance(TimeSpan.FromMinutes(2));

        Assert.False(await cache.ReplaceIfEqualAsync("lease", "owner", "owner", TimeSpan.FromMinutes(10)));
        Assert.False(await cache.ExistsAsync("lease"));
    }

'''
s = s.replace('    [Fact]\n    public async Task RenewAsync_WithCurrentOwner_ExtendsLease()', insert + '    [Fact]\n    public async Task RenewAsync_WithCurrentOwner_ExtendsLease()')
p.write_text(s)
