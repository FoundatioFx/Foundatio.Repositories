using Foundatio.Repositories.Options;
using Xunit;

namespace Foundatio.Repositories.Tests.Options;

public sealed class VersionOptionsTests
{
    [Fact]
    public void ShouldSkipVersionCheck_ByDefault_ReturnsFalse()
    {
        Assert.False(new CommandOptions().ShouldSkipVersionCheck());
    }

    [Fact]
    public void SkipVersionCheck_SkipsTheCheck()
    {
        Assert.True(new CommandOptions().SkipVersionCheck().ShouldSkipVersionCheck());
    }

    [Theory]
    [InlineData(true, false)]
    [InlineData(false, true)]
    public void VersionCheck_WithFlag_ChecksVersionWhenTrue(bool shouldCheckVersion, bool expectedSkip)
    {
        Assert.Equal(expectedSkip, new CommandOptions().VersionCheck(shouldCheckVersion).ShouldSkipVersionCheck());
    }

    [Fact]
    public void IfVersion_StoresExpectedVersion()
    {
        var options = new CommandOptions();
        Assert.False(options.HasExpectedVersion());
        Assert.Null(options.GetExpectedVersion());

        options.IfVersion("1:5");

        Assert.True(options.HasExpectedVersion());
        Assert.Equal("1:5", options.GetExpectedVersion());
    }

    [Fact]
    public void IfVersion_WithNullOrEmpty_ClearsExpectedVersion()
    {
        var options = new CommandOptions().IfVersion("1:5");

        options.IfVersion(null);

        Assert.False(options.HasExpectedVersion());
    }

    [Fact]
    public void GetRetryCount_ByDefault_ReturnsTen()
    {
        var options = new CommandOptions();

        Assert.False(options.HasRetryCount());
        Assert.Equal(10, options.GetRetryCount());
        Assert.Equal(0, new CommandOptions().Retry(0).GetRetryCount());
    }
}
