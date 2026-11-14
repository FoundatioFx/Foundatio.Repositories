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
}
