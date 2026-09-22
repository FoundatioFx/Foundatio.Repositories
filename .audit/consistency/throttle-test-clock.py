from pathlib import Path
import sys
p=Path(sys.argv[1])/'tests/Foundatio.Tests/Locks/InMemoryLockTests.cs'
s=p.read_text();s=s.replace('using System;','using System;\nusing System.Threading;\nusing Microsoft.Extensions.Time.Testing;')
old='''    public override Task WillThrottleCallsAsync()
    {
        return base.WillThrottleCallsAsync();
    }'''
assert s.count(old)==1
s=s.replace(old,'''    public override async Task WillThrottleCallsAsync()
    {
        var time = new FakeTimeProvider(new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));
        using var cache = new InMemoryCacheClient(o => o.TimeProvider(time));
        var period = TimeSpan.FromSeconds(2);
        var provider = new ThrottlingLockProvider(cache, 25, period, time);
        const string resource = "fixed-period";
        for (int i = 0; i < 25; i++)
        {
            await using var lease = await provider.TryAcquireAsync(resource, cancellationToken: TestContext.Current.CancellationToken);
            Assert.NotNull(lease);
        }

        // A cancelled wait probes the exhausted period without waiting for time to advance.
        Assert.Null(await provider.TryAcquireAsync(resource, cancellationToken: new CancellationToken(true)));
        time.Advance(period);
        await using var nextPeriod = await provider.TryAcquireAsync(resource, cancellationToken: TestContext.Current.CancellationToken);
        Assert.NotNull(nextPeriod);
    }''')
p.write_text(s)
