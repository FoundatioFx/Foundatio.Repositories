using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Exceptions;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public partial class IndexCompatibilityTests
{
    [Fact]
    public async Task ConfigureIndexesAsync_DoesNotCheckCompatibility()
    {
        // Arrange
        using var configuration = new ElasticConfiguration();
        using var index = new CountingCompatibilityIndex(configuration);

        // Act
        await configuration.ConfigureIndexesAsync([index]);

        // Assert
        Assert.Equal(0, index.CompatibilityChecks);
    }

    [Fact]
    public void GetCompatibilityIndexPattern_ForDailyIndex_IncludesAllPhysicalPartitions()
    {
        // Arrange
        var index = new TestDailyIndex(new ElasticConfiguration(), "logs", 1);

        // Act
        string pattern = index.GetCompatibilityIndexPatternPublic();

        // Assert
        Assert.Equal("logs-v*-*", pattern);
    }

    [Fact]
    public void GetCompatibilityIndexPattern_ForVersionedIndex_IncludesRetainedPhysicalVersions()
    {
        // Arrange
        var index = new TestVersionedIndex(new ElasticConfiguration(), "employees", 3);

        // Act
        string pattern = index.GetCompatibilityIndexPatternPublic();

        // Assert
        Assert.Equal("employees-v*", pattern);
    }

    [Fact]
    public void GetCompatibilityIndexPattern_ForPlainIndex_IncludesGeneratedErrorPartition()
    {
        // Arrange
        var index = new TestPlainIndex(new ElasticConfiguration(), "employees");

        // Act
        string pattern = index.GetCompatibilityIndexPatternPublic();

        // Assert
        Assert.Equal("employees,employees-error", pattern);
    }

    [Fact]
    public void DailyIndex_GetIndexDate_WithoutRevisionSuffix_ParsesDate()
    {
        // Arrange
        var index = new TestDailyIndex(new ElasticConfiguration(), "logs", 1);

        // Act
        var date = index.GetIndexDatePublic("logs-v1-2023.05.01");

        // Assert
        Assert.Equal(new DateTime(2023, 5, 1, 0, 0, 0, DateTimeKind.Utc), date);
    }

    [Fact]
    public void DailyIndex_GetIndexDate_WithCompatibilityPrefix_StripsPrefixAndParsesDate()
    {
        // Arrange
        var index = new TestDailyIndex(new ElasticConfiguration(), "logs", 1);

        // Act
        var date = index.GetIndexDatePublic("reindexed-v8-logs-v1-2023.05.01");

        // Assert
        Assert.Equal(new DateTime(2023, 5, 1, 0, 0, 0, DateTimeKind.Utc), date);
    }

    [Fact]
    public void DailyIndex_GetIndexDate_WithNewerCompatibilityPrefix_StripsPrefixAndParsesDate()
    {
        // Arrange
        var index = new TestDailyIndex(new ElasticConfiguration(), "logs", 1);

        // Act
        var date = index.GetIndexDatePublic("reindexed-v10-logs-v1-2023.05.01");

        // Assert
        Assert.Equal(new DateTime(2023, 5, 1, 0, 0, 0, DateTimeKind.Utc), date);
    }

    [Fact]
    public void VersionedIndex_GetIndexVersion_WithNaturalCompatibilityPrefixPreservesName()
    {
        // Arrange
        var index = new TestVersionedIndex(new ElasticConfiguration(), "reindexed-v8-events", 1);

        // Act & Assert
        Assert.Equal(1, index.GetIndexVersionPublic("reindexed-v8-events-v1"));
        Assert.Equal(1, index.GetIndexVersionPublic("reindexed-v9-reindexed-v8-events-v1"));
    }

    [Fact]
    public void DailyIndex_GetIndexDate_WithInvalidName_ReturnsMaxValue()
    {
        // Arrange
        var index = new TestDailyIndex(new ElasticConfiguration(), "logs", 1);

        // Act
        var date = index.GetIndexDatePublic("not-a-matching-name");

        // Assert
        Assert.Equal(DateTime.MaxValue, date);
    }

    [Fact]
    public void MatchesCompatibilitySource_WhenSiblingNameEndsInError_YieldsToExactSibling()
    {
        // Arrange
        using var configuration = new ElasticConfiguration();
        using var events = new Index<object>(configuration, "events");
        using var errors = new Index<object>(configuration, "events-error");
        configuration.AddIndex(events);
        configuration.AddIndex(errors);
        var aliases = new Dictionary<string, Alias>
        {
            [ElasticReindexer.ErrorIndexOwnershipAlias] = new() { IsHidden = true }
        };

        // Act & Assert
        Assert.False(events.MatchesCompatibilitySource(errors.Name, aliases));
        Assert.True(errors.MatchesCompatibilitySource(errors.Name, aliases));
    }

    [Fact]
    public void MatchesCompatibilitySource_WhenCustomSiblingExactlyNamesCandidate_YieldsToSibling()
    {
        // Arrange
        using var configuration = new ElasticConfiguration();
        using var events = new VersionedIndex<object>(configuration, "events", 1);
        using var custom = new MinimalIndex(configuration, events.VersionedName);
        configuration.AddIndex(events);
        configuration.AddIndex(custom);

        // Act & Assert: a registered sibling exactly named after the candidate takes ownership precedence
        Assert.False(events.MatchesCompatibilitySource(events.VersionedName, new Dictionary<string, Alias>()));
    }

    [Fact]
    public void HasExactHiddenAlias_RequiresExactMarkerDefinition()
    {
        // Arrange & Act & Assert
        Assert.True(new Dictionary<string, Alias>
        {
            [ElasticReindexer.ErrorIndexOwnershipAlias] = new() { IsHidden = true }
        }.HasExactHiddenAlias(ElasticReindexer.ErrorIndexOwnershipAlias));
        Assert.False(new Dictionary<string, Alias>
        {
            [ElasticReindexer.ErrorIndexOwnershipAlias] = new() { IsHidden = false }
        }.HasExactHiddenAlias(ElasticReindexer.ErrorIndexOwnershipAlias));
        Assert.False(new Dictionary<string, Alias>
        {
            [ElasticReindexer.ErrorIndexOwnershipAlias] = new() { IsHidden = true, Routing = "foreign" }
        }.HasExactHiddenAlias(ElasticReindexer.ErrorIndexOwnershipAlias));
    }

    [Fact]
    public void HasCanonicalCompatibilityAlias_RejectsWriteAndRoutingSemantics()
    {
        // Arrange
        const string canonicalName = "employees-v1";

        // Act & Assert
        Assert.True(new Dictionary<string, Alias>
        {
            [canonicalName] = new() { IsHidden = true }
        }.HasCanonicalCompatibilityAlias(canonicalName));
        Assert.False(new Dictionary<string, Alias>
        {
            [canonicalName] = new() { IsWriteIndex = true }
        }.HasCanonicalCompatibilityAlias(canonicalName));
        Assert.False(new Dictionary<string, Alias>
        {
            [canonicalName] = new() { IsWriteIndex = false }
        }.HasCanonicalCompatibilityAlias(canonicalName));
        Assert.False(new Dictionary<string, Alias>
        {
            [canonicalName] = new() { Routing = "foreign" }
        }.HasCanonicalCompatibilityAlias(canonicalName));
    }

    [Fact]
    public void MatchesCompatibilitySource_WithNonCanonicalAliasDefinition_Throws()
    {
        // Arrange
        using var index = new VersionedIndex<object>(new ElasticConfiguration(), "employees", 1);
        var aliases = new Dictionary<string, Alias>
        {
            [index.VersionedName] = new() { IsWriteIndex = false }
        };

        // Act
        var exception = Assert.Throws<RepositoryException>(() =>
            index.MatchesCompatibilitySource($"reindexed-v9-{index.VersionedName}", aliases));

        // Assert
        Assert.Contains("non-canonical definition", exception.Message);
    }

    [Fact]
    public void MatchesCompatibilitySource_WhenSelfNotRegistered_KeepsStructuralOwnership()
    {
        // Arrange
        using var configuration = new ElasticConfiguration();
        using var events = new VersionedIndex<object>(configuration, "events", 1);

        // Act & Assert
        Assert.True(events.MatchesCompatibilitySource(events.VersionedName, new Dictionary<string, Alias>()));
    }
}
