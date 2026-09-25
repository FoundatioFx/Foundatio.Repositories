using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Foundatio.Repositories.Elasticsearch.Jobs;
using Foundatio.Repositories.Exceptions;
using Foundatio.Serializer;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class ElasticReindexerTests
{
    [Theory]
    [InlineData(404)]
    [InlineData(500)]
    public Task GetIndexAliasesAsync_WhenMetadataCannotBeRead_Throws(int statusCode)
    {
        // Arrange
        var requestInvoker = new Elastic.Transport.InMemoryRequestInvoker(Encoding.UTF8.GetBytes("{}"), statusCode, null, "application/json");
        var client = new ElasticsearchClient(new ElasticsearchClientSettings(requestInvoker));
        var reindexer = new ElasticReindexer(client, new SystemTextJsonSerializer());

        // Act & Assert
        return Assert.ThrowsAsync<RepositoryException>(() => reindexer.GetIndexAliasesAsync("employees"));
    }

    [Fact]
    public void GetNoProgressTimeout_WhenBatchSizeNotSpecified_UsesElasticsearchDefaultBatchSize()
    {
        // Arrange
        // No ReindexBatchSize means Elasticsearch applies its own default of 1000 docs/batch, so the
        // expected inter-batch pause is 1000 docs / 1 doc-per-sec = 1000s, extended 3x to 3000s (50 min).
        var workItem = new ReindexWorkItem { OldIndex = "old", NewIndex = "new", Alias = "alias", ReindexRequestsPerSecond = 1 };
        var expectedTimeout = TimeSpan.FromSeconds(3000);

        // Act
        var timeout = ElasticReindexer.GetNoProgressTimeout(workItem);

        // Assert
        Assert.Equal(expectedTimeout, timeout);
    }

    [Fact]
    public void GetNoProgressTimeout_WhenNoThrottleConfigured_ReturnsDefaultTimeout()
    {
        // Arrange
        var workItem = new ReindexWorkItem { OldIndex = "old", NewIndex = "new", Alias = "alias" };
        var expectedTimeout = TimeSpan.FromMinutes(10);

        // Act
        var timeout = ElasticReindexer.GetNoProgressTimeout(workItem);

        // Assert
        Assert.Equal(expectedTimeout, timeout);
    }

    [Fact]
    public void GetNoProgressTimeout_WhenThrottleIsGenerous_ReturnsDefaultTimeout()
    {
        // Arrange
        // 1000 docs/batch at 10000 docs/sec is an expected pause of 0.1s - nowhere near the 10 minute
        // default, so the default should still apply rather than shrinking the timeout.
        var workItem = new ReindexWorkItem { OldIndex = "old", NewIndex = "new", Alias = "alias", ReindexBatchSize = 1000, ReindexRequestsPerSecond = 10000 };
        var expectedTimeout = TimeSpan.FromMinutes(10);

        // Act
        var timeout = ElasticReindexer.GetNoProgressTimeout(workItem);

        // Assert
        Assert.Equal(expectedTimeout, timeout);
    }

    [Fact]
    public void GetNoProgressTimeout_WhenThrottleIsRestrictive_ExtendsTimeoutBeyondDefault()
    {
        // Arrange
        // 500 docs/batch at 2 docs/sec is an expected pause of 250s, extended 3x to 750s (12.5 min) -
        // beyond the 10 minute default, so a healthy but slow, intentionally throttled reindex isn't
        // mistaken for a stall and cancelled.
        var workItem = new ReindexWorkItem { OldIndex = "old", NewIndex = "new", Alias = "alias", ReindexBatchSize = 500, ReindexRequestsPerSecond = 2 };
        var expectedTimeout = TimeSpan.FromSeconds(750);

        // Act
        var timeout = ElasticReindexer.GetNoProgressTimeout(workItem);

        // Assert
        Assert.Equal(expectedTimeout, timeout);
    }

    [Fact]
    public void GetNoProgressTimeout_WhenThrottleWouldOverflowTimeSpan_ReturnsMaxValue()
    {
        // Arrange
        // int.MaxValue docs/batch at 0.001 docs/sec is an expected pause of ~2.15 billion seconds,
        // extended 3x to ~6.44 trillion seconds - far beyond TimeSpan's ~29,247 year (~922 billion
        // second) range. Computing this via TimeSpan arithmetic (rather than clamping in double space
        // first) would throw OverflowException instead of returning a usable timeout.
        var workItem = new ReindexWorkItem { OldIndex = "old", NewIndex = "new", Alias = "alias", ReindexBatchSize = int.MaxValue, ReindexRequestsPerSecond = 0.001f };

        // Act
        var timeout = ElasticReindexer.GetNoProgressTimeout(workItem);

        // Assert
        Assert.Equal(TimeSpan.MaxValue, timeout);
    }

    [Fact]
    public void GetStatusRetryDelay_WhenCalledRepeatedly_ProducesVariedDelays()
    {
        // Arrange
        const int failedAttempts = 4;

        // Act
        // With jitter enabled, repeated calls for the same attempt count should not all return
        // the exact same delay - otherwise concurrent callers would still retry in lockstep.
        var delays = Enumerable.Range(0, 25).Select(_ => ElasticReindexer.GetStatusRetryDelay(failedAttempts)).Distinct().ToList();

        // Assert
        Assert.True(delays.Count > 1, "Expected jitter to produce varied delays across repeated calls.");
    }

    [Theory]
    [InlineData(10)]
    [InlineData(100)]
    public void GetStatusRetryDelay_WhenExponentialDelayFarExceedsMax_IsAlwaysCapped(int failedAttempts)
    {
        // Arrange
        // Nominal delay here is so far beyond the cap that even the lowest end of the jitter range
        // (0.75x) still exceeds it, so the result is deterministically capped at exactly 30s.
        var expectedDelay = TimeSpan.FromSeconds(30);

        for (int i = 0; i < 25; i++)
        {
            // Act
            var delay = ElasticReindexer.GetStatusRetryDelay(failedAttempts);

            // Assert
            Assert.Equal(expectedDelay, delay);
        }
    }

    [Fact]
    public void GetStatusRetryDelay_WhenExponentialDelayNearMax_IsCappedButMayVary()
    {
        // Arrange
        // At 6 failed attempts the nominal delay (32s) is close enough to the 30s cap that jitter's
        // lower bound (24s) can fall under it, so the result isn't always exactly 30s - just never over it.
        const int failedAttempts = 6;

        for (int i = 0; i < 25; i++)
        {
            // Act
            var delay = ElasticReindexer.GetStatusRetryDelay(failedAttempts);

            // Assert
            Assert.InRange(delay, TimeSpan.FromSeconds(24), TimeSpan.FromSeconds(30));
        }
    }

    [Theory]
    [InlineData(1, 1)]
    [InlineData(2, 2)]
    [InlineData(3, 4)]
    [InlineData(4, 8)]
    [InlineData(5, 16)]
    public void GetStatusRetryDelay_WithFailedAttempts_GrowsExponentially(int failedAttempts, int nominalSeconds)
    {
        // Arrange
        // The delay includes +/-25% jitter, so assertions check a range around the nominal
        // (un-jittered) exponential value rather than an exact match.
        var minDelay = TimeSpan.FromSeconds(nominalSeconds * 0.75);
        var maxDelay = TimeSpan.FromSeconds(nominalSeconds * 1.25);

        for (int i = 0; i < 25; i++)
        {
            // Act
            var delay = ElasticReindexer.GetStatusRetryDelay(failedAttempts);

            // Assert
            Assert.InRange(delay, minDelay, maxDelay);
        }
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void GetStatusRetryDelay_WithNonPositiveAttempts_ReturnsBaseDelay(int failedAttempts)
    {
        // Arrange
        var minDelay = TimeSpan.FromSeconds(0.75);
        var maxDelay = TimeSpan.FromSeconds(1.25);

        for (int i = 0; i < 25; i++)
        {
            // Act
            var delay = ElasticReindexer.GetStatusRetryDelay(failedAttempts);

            // Assert
            Assert.InRange(delay, minDelay, maxDelay);
        }
    }

    [Theory]
    [InlineData(float.PositiveInfinity)]
    [InlineData(float.NegativeInfinity)]
    public Task ReindexAsync_WithInfiniteRequestsPerSecond_ThrowsArgumentOutOfRangeException(float requestsPerSecond)
    {
        // Arrange
        var reindexer = new ElasticReindexer(null!, new SystemTextJsonSerializer());
        var workItem = new ReindexWorkItem { OldIndex = "old", NewIndex = "new", Alias = "alias", ReindexRequestsPerSecond = requestsPerSecond };

        // Act & Assert
        return Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => reindexer.ReindexAsync(workItem));
    }

    [Fact]
    public Task ReindexAsync_WithNaNRequestsPerSecond_ThrowsArgumentOutOfRangeException()
    {
        // Arrange
        var reindexer = new ElasticReindexer(null!, new SystemTextJsonSerializer());
        var workItem = new ReindexWorkItem { OldIndex = "old", NewIndex = "new", Alias = "alias", ReindexRequestsPerSecond = float.NaN };

        // Act & Assert
        return Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => reindexer.ReindexAsync(workItem));
    }

    [Fact]
    public Task ReindexAsync_WithNonPositiveBatchSize_ThrowsArgumentOutOfRangeException()
    {
        // Arrange
        var reindexer = new ElasticReindexer(null!, new SystemTextJsonSerializer());
        var workItem = new ReindexWorkItem { OldIndex = "old", NewIndex = "new", Alias = "alias", ReindexBatchSize = 0 };

        // Act & Assert
        return Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => reindexer.ReindexAsync(workItem));
    }

    [Fact]
    public Task ReindexAsync_WithNonPositiveRequestsPerSecond_ThrowsArgumentOutOfRangeException()
    {
        // Arrange
        var reindexer = new ElasticReindexer(null!, new SystemTextJsonSerializer());
        var workItem = new ReindexWorkItem { OldIndex = "old", NewIndex = "new", Alias = "alias", ReindexRequestsPerSecond = -1 };

        // Act & Assert
        return Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => reindexer.ReindexAsync(workItem));
    }

    [Fact]
    public Task ReindexAsync_WithNullWorkItem_ThrowsArgumentNullException()
    {
        // Arrange
        var reindexer = new ElasticReindexer(null!, new SystemTextJsonSerializer());

        // Act & Assert
        return Assert.ThrowsAsync<ArgumentNullException>(() => reindexer.ReindexAsync(null!));
    }
}
