using System;
using System.Collections.Generic;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class ElasticDocumentVersionTests
{
    [Theory]
    [InlineData("1:0", 1, 0)]
    [InlineData("1:42", 1, 42)]
    [InlineData("3:9007199254740993", 3, 9007199254740993)]
    public void Parse_WithValidVersion_ReturnsPrimaryTermAndSequenceNumber(string value, long primaryTerm, long sequenceNumber)
    {
        var version = ElasticDocumentVersion.Parse(value);

        Assert.False(version.IsEmpty);
        Assert.Equal(primaryTerm, version.PrimaryTerm);
        Assert.Equal(sequenceNumber, version.SequenceNumber);
        Assert.Equal(value, version.ToString());
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("abc")]
    [InlineData("1")]
    [InlineData("1:2:3")]
    [InlineData("a:1")]
    [InlineData("0:0")]
    [InlineData("0:-2")]
    [InlineData("0:5")]
    [InlineData("1:-1")]
    [InlineData("-1:4")]
    public void Parse_WithMissingMalformedOrSentinelVersion_ReturnsEmpty(string? value)
    {
        var version = ElasticDocumentVersion.Parse(value!);

        Assert.True(version.IsEmpty);
        Assert.Equal(ElasticDocumentVersion.Empty, version);
    }

    [Theory]
    [InlineData(0, -2)]
    [InlineData(0, 0)]
    [InlineData(1, -2)]
    [InlineData(0, 7)]
    public void IsEmpty_WithSequenceNumbersDisabledSentinelOrUnassigned_ReturnsTrue(long primaryTerm, long sequenceNumber)
    {
        Assert.True(new ElasticDocumentVersion(primaryTerm, sequenceNumber).IsEmpty);
    }

    [Fact]
    public void Equals_WithBothEmpty_ReturnsTrueAndMatchingHashCodes()
    {
        var empty = ElasticDocumentVersion.Empty;
        var sentinel = new ElasticDocumentVersion(0, -2);

        Assert.True(empty.Equals(sentinel));
        Assert.True(empty == sentinel);
        Assert.False(empty != sentinel);
        Assert.Equal(empty.GetHashCode(), sentinel.GetHashCode());
        Assert.Equal(0, empty.CompareTo(sentinel));
    }

    [Fact]
    public void Equals_WithEmptyAndNonEmpty_ReturnsFalse()
    {
        var version = new ElasticDocumentVersion(1, 0);

        Assert.False(version.Equals(ElasticDocumentVersion.Empty));
        Assert.False(ElasticDocumentVersion.Empty.Equals(version));
        Assert.True(version != ElasticDocumentVersion.Empty);
    }

    [Fact]
    public void Equals_WithSameValues_ReturnsTrueAndMatchingHashCodes()
    {
        var a = new ElasticDocumentVersion(2, 15);
        var b = ElasticDocumentVersion.Parse("2:15");

        Assert.True(a.Equals(b));
        Assert.True(a.Equals((object)b));
        Assert.True(a.Equals("2:15"));
        Assert.True(a == b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());

        var set = new HashSet<ElasticDocumentVersion> { a, b, ElasticDocumentVersion.Empty, new(0, -2) };
        Assert.Equal(2, set.Count);
    }

    [Fact]
    public void Equals_WithVersionedDocument_ComparesDocumentVersion()
    {
        var employee = new Employee { Version = "1:3" };

        Assert.True(new ElasticDocumentVersion(1, 3).Equals((object)employee));
        Assert.Equal(new ElasticDocumentVersion(1, 3), employee.GetElasticVersion());
    }

    [Fact]
    public void CompareTo_OrdersByPrimaryTermThenSequenceNumber()
    {
        var versions = new List<ElasticDocumentVersion>
        {
            new(2, 1),
            new(1, 100),
            ElasticDocumentVersion.Empty,
            new(1, 2),
            new(2, 0)
        };

        versions.Sort();

        Assert.Equal(new[] { "0:0", "1:2", "1:100", "2:0", "2:1" }, versions.ConvertAll(v => v.ToString()));
        Assert.True(new ElasticDocumentVersion(1, 2) < new ElasticDocumentVersion(1, 3));
        Assert.True(new ElasticDocumentVersion(2, 0) > new ElasticDocumentVersion(1, 999));
        Assert.True(new ElasticDocumentVersion(1, 2) <= ElasticDocumentVersion.Parse("1:2"));
        Assert.True(new ElasticDocumentVersion(1, 2) >= ElasticDocumentVersion.Parse("1:2"));
    }

    [Fact]
    public void CompareTo_WithEmpty_SortsEmptyFirst()
    {
        var version = new ElasticDocumentVersion(1, 0);

        Assert.True(ElasticDocumentVersion.Empty < version);
        Assert.True(new ElasticDocumentVersion(0, -2) < version);
        Assert.Equal(1, version.CompareTo((object?)null));
        Assert.Equal(0, version.CompareTo((object)"1:0"));
    }

    [Fact]
    public void CompareTo_WithUnsupportedType_Throws()
    {
        Assert.Throws<ArgumentException>(() => new ElasticDocumentVersion(1, 0).CompareTo(42));
    }

    [Fact]
    public void ImplicitConversions_RoundTrip()
    {
        ElasticDocumentVersion version = "4:12";
        string? value = version;
        string? empty = ElasticDocumentVersion.Empty;
        string? sentinel = new ElasticDocumentVersion(0, -2);

        Assert.Equal(4, version.PrimaryTerm);
        Assert.Equal(12, version.SequenceNumber);
        Assert.Equal("4:12", value);
        Assert.Null(empty);
        Assert.Null(sentinel);
        Assert.Equal(ElasticDocumentVersion.Empty.ToVersionString(), empty);
    }

    [Fact]
    public void ToVersionString_WithEmpty_ReturnsNull()
    {
        Assert.Null(ElasticDocumentVersion.Empty.ToVersionString());
        Assert.Null(new ElasticDocumentVersion(0, -2).ToVersionString());
        Assert.Equal("1:5", new ElasticDocumentVersion(1, 5).ToVersionString());
    }
}
