using System;
using System.Threading.Tasks;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public partial class IndexCompatibilityTests
{
    [Theory]
    [InlineData("plain", "reindexed-v8-events-error")]
    [InlineData("versioned", "reindexed-v8-events-v1-error")]
    [InlineData("daily", "reindexed-v8-events-v1-2024.01.15-error")]
    [InlineData("monthly", "reindexed-v8-events-v1-2024.01-error")]
    [InlineData("literal", "reindexed-v1-error")]
    public void ErrorLineage_PreservesNativePrefixes(string kind, string nativeError)
    {
        // Arrange
        using var configuration = new ElasticConfiguration();
        using var index = CreateRecoveryErrorIndex(configuration, kind);
        string wrappedError = CompatibilityIndexName.Create(nativeError, 9, index.Name);

        // Act & Assert
        Assert.True(index.IsPotentialCompatibilityErrorName(nativeError));
        Assert.True(index.IsPotentialCompatibilityErrorName(wrappedError));
    }

    [Theory]
    [InlineData("plain", "reindexed-v8-events-error", false)]
    [InlineData("plain", "reindexed-v8-events-error", true)]
    [InlineData("versioned", "reindexed-v8-events-v1-error", false)]
    [InlineData("versioned", "reindexed-v8-events-v1-error", true)]
    [InlineData("daily", "reindexed-v8-events-v1-2024.01.15-error", false)]
    [InlineData("daily", "reindexed-v8-events-v1-2024.01.15-error", true)]
    [InlineData("monthly", "reindexed-v8-events-v1-2024.01-error", false)]
    [InlineData("monthly", "reindexed-v8-events-v1-2024.01-error", true)]
    [InlineData("literal", "reindexed-v1-error", false)]
    [InlineData("literal", "reindexed-v1-error", true)]
    public async Task ErrorLineage_RequiresProvenanceOnSurvivingTarget(string kind, string nativeError, bool errorMarker)
    {
        // Arrange
        string marker = errorMarker ? ",\".foundatio-reindex-error\":{\"is_hidden\":true}" : String.Empty;
        string target = $"reindexed-v9-{nativeError}";
        var invoker = new SequenceRequestInvoker(
            new StubResponse(200, """{"version":{"number":"9.0.0"}}"""),
            new StubResponse(200, """
                {"TARGET":{"aliases":{"SOURCE":{},".foundatio-compatibility-upgrade":{"is_hidden":true}MARKER},"settings":{"index":{"blocks":{"write":true}}}}}
                """.Replace("TARGET", target, StringComparison.Ordinal)
                    .Replace("SOURCE", nativeError, StringComparison.Ordinal)
                    .Replace("MARKER", marker, StringComparison.Ordinal)),
            new StubResponse(200, """{"nodes":{}}"""));
        using var configuration = new RequestInvokerElasticConfiguration(invoker);
        var index = CreateRecoveryErrorIndex(configuration, kind);
        configuration.AddIndex(index);

        // Act
        var status = await configuration.InspectIndexCompatibilityUpgradeAsync(index, nativeError, TestContext.Current.CancellationToken);

        // Assert
        Assert.Equal(errorMarker, status.ErrorLineageAuthenticated);
        Assert.Equal(errorMarker ? IndexCompatibilityRecoveryAction.Finish : IndexCompatibilityRecoveryAction.ManualIntervention, status.Action);
        Assert.True(status.TargetWriteBlocked);
        Assert.False(status.SourceExists);
        Assert.Equal(3, invoker.Requests.Count);
        Assert.All(invoker.Requests, request => Assert.StartsWith("GET ", request, StringComparison.Ordinal));
        Assert.Equal(0, invoker.RemainingResponses);
    }

    [Theory]
    [InlineData("orders-error")]
    [InlineData("reindexed-v8-orders-error")]
    public void ErrorLineage_DoesNotClaimNaturalErrorSuffix(string name)
    {
        // Arrange
        using var configuration = new ElasticConfiguration();
        using var index = new Index<object>(configuration, name);

        // Act & Assert
        Assert.False(index.IsPotentialCompatibilityErrorName(name));
        Assert.False(index.IsPotentialCompatibilityErrorName(CompatibilityIndexName.Create(name, 9, name)));
    }

    private static Configuration.Index CreateRecoveryErrorIndex(IElasticConfiguration configuration, string kind)
    {
        return kind switch
        {
            "plain" => new Index<object>(configuration, "reindexed-v8-events"),
            "versioned" => new VersionedIndex<object>(configuration, "reindexed-v8-events"),
            "daily" => new DailyIndex<object>(configuration, "reindexed-v8-events"),
            "monthly" => new MonthlyIndex<object>(configuration, "reindexed-v8-events"),
            "literal" => new VersionedIndex<object>(configuration, "reindexed"),
            _ => throw new ArgumentOutOfRangeException(nameof(kind))
        };
    }
}
