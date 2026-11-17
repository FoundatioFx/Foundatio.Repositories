using Foundatio.Repositories.Options;
using Xunit;

namespace Foundatio.Repositories.Tests.Options;

public sealed class CacheOptionsTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ReadCacheFalse_OnClonedOptions_DisablesInheritedReadsWithoutChangingWritesOrTheOriginal(bool cacheWrites)
    {
        var original = new CommandOptions().Cache(cacheWrites).ReadCache();
        var clone = original.Clone();

        clone.ReadCache(false);

        Assert.False(clone.ShouldReadCache());
        Assert.Equal(cacheWrites, clone.ShouldUseCache());
        Assert.True(original.ShouldReadCache());
        Assert.Equal(cacheWrites, original.ShouldUseCache());
    }

    [Fact]
    public void ReadCache_WithoutArguments_EnablesReadsWithoutEnablingWrites()
    {
        var options = new CommandOptions().Cache(false).ReadCache(false);

        options.ReadCache();

        Assert.True(options.ShouldReadCache());
        Assert.False(options.ShouldUseCache());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Cache_WithoutReadOverride_ControlsBothReadsAndWrites(bool enabled)
    {
        var options = new CommandOptions().Cache(enabled);

        Assert.Equal(enabled, options.ShouldReadCache());
        Assert.Equal(enabled, options.ShouldUseCache());
    }

    [Theory]
    [InlineData(false, false, null)]
    [InlineData(false, true, null)]
    [InlineData(true, false, null)]
    [InlineData(true, true, null)]
    [InlineData(false, false, false)]
    [InlineData(false, true, false)]
    [InlineData(true, false, false)]
    [InlineData(true, true, false)]
    [InlineData(false, false, true)]
    [InlineData(false, true, true)]
    [InlineData(true, false, true)]
    [InlineData(true, true, true)]
    public void ShouldReadCache_WithExplicitDefault_PreservesReadOverrideAndCacheSetting(bool cacheEnabled, bool defaultValue, bool? readOverride)
    {
        var options = new CommandOptions().Cache(cacheEnabled);
        if (readOverride is { } enabled)
            options.ReadCache(enabled);

        Assert.Equal(readOverride ?? defaultValue, options.ShouldReadCache(defaultValue));
        Assert.Equal(readOverride ?? cacheEnabled, options.ShouldReadCache());
        Assert.Equal(cacheEnabled, options.ShouldUseCache());
    }
}
