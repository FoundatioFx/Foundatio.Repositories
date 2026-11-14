using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using Xunit;

namespace Foundatio.Repositories.Tests;

public class IdsTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public void ConvertToList_PreservesOrderAndRouting(int count)
    {
        var ids = new Ids(new[] { new Id("first"), new Id("second", "parent") }.Take(count));

        List<string> values = ids;

        Assert.Equal(new[] { "first", "parent-second" }.Take(count), values);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public void ConvertToCollection_PreservesOrderAndRouting(int count)
    {
        var ids = new Ids(new[] { new Id("first"), new Id("second", "parent") }.Take(count));

        Collection<string> values = ids;

        Assert.Equal(new[] { "first", "parent-second" }.Take(count), values);
    }
}
