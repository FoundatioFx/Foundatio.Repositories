using Foundatio.Repositories.Options;
using Xunit;

namespace Foundatio.Repositories.Tests;

public sealed class CacheOptionsTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ReadCache_FollowsCacheSettingUnlessExplicitlyOverridden(bool cacheEnabled)
    {
        var options = new CommandOptions().Cache(cacheEnabled);

        Assert.Equal(cacheEnabled, options.ShouldReadCache());
        Assert.Same(options, options.ReadCache());
        Assert.True(options.ShouldReadCache());
        Assert.Equal(cacheEnabled, options.ShouldUseCache());
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public void ReadCache_ExplicitSettingDoesNotChangeCacheWrites(bool cacheEnabled, bool readEnabled)
    {
        var options = new CommandOptions().Cache(cacheEnabled).ReadCache();

        Assert.Same(options, options.ReadCache(readEnabled));
        Assert.Equal(readEnabled, options.ShouldReadCache());
        Assert.Equal(cacheEnabled, options.ShouldUseCache());

        options.Cache(!cacheEnabled);

        Assert.Equal(readEnabled, options.ShouldReadCache());
        Assert.Equal(!cacheEnabled, options.ShouldUseCache());
    }

    [Fact]
    public void DisablingCacheOnClone_DoesNotChangeCallerOptions()
    {
        var options = new CommandOptions().Cache("documents").ReadCache();

        var clone = options.Clone().Cache(false).ReadCache(false);

        Assert.False(clone.ShouldUseCache());
        Assert.False(clone.ShouldReadCache());
        Assert.Equal("documents", clone.GetCacheKey());
        Assert.True(options.ShouldUseCache());
        Assert.True(options.ShouldReadCache());
        Assert.Equal("documents", options.GetCacheKey());
    }
}
