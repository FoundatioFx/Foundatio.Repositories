using System;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Repositories.Elasticsearch.Jobs;
using Foundatio.Serializer;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class ElasticReindexerTests
{
    // The delay includes +/-25% jitter, so assertions check a range around the nominal (un-jittered)
    // exponential value rather than an exact match.
    [Theory]
    [InlineData(1, 1)]
    [InlineData(2, 2)]
    [InlineData(3, 4)]
    [InlineData(4, 8)]
    [InlineData(5, 16)]
    public void GetStatusRetryDelay_WithFailedAttempts_GrowsExponentially(int failedAttempts, int nominalSeconds)
    {
        for (int i = 0; i < 25; i++)
        {
            var delay = ElasticReindexer.GetStatusRetryDelay(failedAttempts);

            Assert.InRange(delay, TimeSpan.FromSeconds(nominalSeconds * 0.75), TimeSpan.FromSeconds(nominalSeconds * 1.25));
        }
    }

    [Theory]
    [InlineData(10)]
    [InlineData(100)]
    public void GetStatusRetryDelay_WhenExponentialDelayFarExceedsMax_IsAlwaysCapped(int failedAttempts)
    {
        // Nominal delay here is so far beyond the cap that even the lowest end of the jitter range
        // (0.75x) still exceeds it, so the result is deterministically capped at exactly 30s.
        for (int i = 0; i < 25; i++)
        {
            var delay = ElasticReindexer.GetStatusRetryDelay(failedAttempts);

            Assert.Equal(TimeSpan.FromSeconds(30), delay);
        }
    }

    [Fact]
    public void GetStatusRetryDelay_WhenExponentialDelayNearMax_IsCappedButMayVary()
    {
        // At 6 failed attempts the nominal delay (32s) is close enough to the 30s cap that jitter's
        // lower bound (24s) can fall under it, so the result isn't always exactly 30s - just never over it.
        for (int i = 0; i < 25; i++)
        {
            var delay = ElasticReindexer.GetStatusRetryDelay(6);

            Assert.InRange(delay, TimeSpan.FromSeconds(24), TimeSpan.FromSeconds(30));
        }
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void GetStatusRetryDelay_WithNonPositiveAttempts_ReturnsBaseDelay(int failedAttempts)
    {
        for (int i = 0; i < 25; i++)
        {
            var delay = ElasticReindexer.GetStatusRetryDelay(failedAttempts);

            Assert.InRange(delay, TimeSpan.FromSeconds(0.75), TimeSpan.FromSeconds(1.25));
        }
    }

    [Fact]
    public void GetStatusRetryDelay_ProducesVariedDelays_AcrossCalls()
    {
        // With jitter enabled, repeated calls for the same attempt count should not all return
        // the exact same delay - otherwise concurrent callers would still retry in lockstep.
        var delays = Enumerable.Range(0, 25).Select(_ => ElasticReindexer.GetStatusRetryDelay(4)).Distinct().ToList();

        Assert.True(delays.Count > 1, "Expected jitter to produce varied delays across repeated calls.");
    }

    [Fact]
    public Task ReindexAsync_WithNonPositiveBatchSize_ThrowsArgumentOutOfRangeException()
    {
        var reindexer = new ElasticReindexer(null!, new SystemTextJsonSerializer());
        var workItem = new ReindexWorkItem { OldIndex = "old", NewIndex = "new", Alias = "alias", ReindexBatchSize = 0 };

        return Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => reindexer.ReindexAsync(workItem));
    }

    [Fact]
    public Task ReindexAsync_WithNonPositiveRequestsPerSecond_ThrowsArgumentOutOfRangeException()
    {
        var reindexer = new ElasticReindexer(null!, new SystemTextJsonSerializer());
        var workItem = new ReindexWorkItem { OldIndex = "old", NewIndex = "new", Alias = "alias", ReindexRequestsPerSecond = -1 };

        return Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => reindexer.ReindexAsync(workItem));
    }

    [Fact]
    public Task ReindexAsync_WithNaNRequestsPerSecond_ThrowsArgumentOutOfRangeException()
    {
        var reindexer = new ElasticReindexer(null!, new SystemTextJsonSerializer());
        var workItem = new ReindexWorkItem { OldIndex = "old", NewIndex = "new", Alias = "alias", ReindexRequestsPerSecond = float.NaN };

        return Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => reindexer.ReindexAsync(workItem));
    }
}
