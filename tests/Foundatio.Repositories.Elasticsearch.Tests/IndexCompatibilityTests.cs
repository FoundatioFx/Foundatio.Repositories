using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Exceptions;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public class IndexCompatibilityTests
{
    [Fact]
    public async Task RunCompatibilityReindexAsync_WhenStartTransportFails_ThrowsUncertainException()
    {
        // Arrange
        var transportException = new TimeoutException("The reindex response was not received.");
        var requestInvoker = new InMemoryRequestInvoker([], 500, transportException, "application/json");
        var client = new ElasticsearchClient(new ElasticsearchClientSettings(requestInvoker));
        var runner = new ElasticReindexTaskRunner(client, new Foundatio.Serializer.SystemTextJsonSerializer(), TimeProvider.System);

        // Act
        var exception = await Assert.ThrowsAsync<ElasticReindexTaskUncertainException>(() => runner.RunCompatibilityReindexAsync(
            "employees-v1", "reindexed-v9-employees-v1", null, null, (_, _) => Task.CompletedTask, CancellationToken.None));

        // Assert
        Assert.Contains("is unknown", exception.Message);
        Assert.Same(transportException, exception.InnerException);
    }

    [Fact]
    public async Task RunCompatibilityReindexAsync_WhenAcceptedWithoutTaskId_ThrowsUncertainException()
    {
        // Arrange
        var headers = new Dictionary<string, IEnumerable<string>> { ["x-elastic-product"] = ["Elasticsearch"] };
        var requestInvoker = new InMemoryRequestInvoker(Encoding.UTF8.GetBytes("{}"), 200, null, "application/json", headers);
        var client = new ElasticsearchClient(new ElasticsearchClientSettings(requestInvoker));
        var runner = new ElasticReindexTaskRunner(client, new Foundatio.Serializer.SystemTextJsonSerializer(), TimeProvider.System);

        // Act
        var exception = await Assert.ThrowsAsync<ElasticReindexTaskUncertainException>(() => runner.RunCompatibilityReindexAsync(
            "employees-v1", "reindexed-v9-employees-v1", null, null, (_, _) => Task.CompletedTask, CancellationToken.None));

        // Assert
        Assert.Contains("no task ID", exception.Message);
    }

    [Theory]
    [InlineData(0, null)]
    [InlineData(-1, null)]
    [InlineData(null, 0f)]
    [InlineData(null, -1f)]
    [InlineData(null, float.PositiveInfinity)]
    [InlineData(null, float.NaN)]
    public void ValidateCompatibilityReindexOptions_WithInvalidThrottle_Throws(int? batchSize, float? requestsPerSecond)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => ElasticReindexTaskRunner.ValidateOptions(batchSize, requestsPerSecond));
    }

    [Fact]
    public async Task ValidateCompatibilityAsync_WithInvalidThrottle_ThrowsBeforeElasticsearchRequest()
    {
        // Arrange
        var requestInvoker = new InMemoryRequestInvoker([], 500, null, "application/json");
        var client = new ElasticsearchClient(new ElasticsearchClientSettings(requestInvoker));
        var upgrader = new ElasticIndexCompatibilityUpgrader(client, new Foundatio.Serializer.SystemTextJsonSerializer(), TimeProvider.System);
        using var index = new Index<object>(new ElasticConfiguration(), "employees") { ReindexBatchSize = 0 };
        var compatibility = new IndexCompatibilityInfo
        {
            Name = "employees",
            CreatedMajor = 8,
            ServerMajor = 9,
            ServerVersion = "9.0.0"
        };

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => upgrader.ValidateAsync(index, compatibility, CancellationToken.None));
    }

    [Theory]
    [InlineData(9, 9, IndexCompatibilityState.Current)]
    [InlineData(8, 9, IndexCompatibilityState.RequiresReindex)]
    [InlineData(7, 9, IndexCompatibilityState.Unsupported)]
    [InlineData(10, 9, IndexCompatibilityState.Unsupported)]
    public void IndexCompatibilityInfo_State_IsDerivedFromMajorVersions(int createdMajor, int serverMajor, IndexCompatibilityState expected)
    {
        // Arrange
        var compatibility = new IndexCompatibilityInfo
        {
            Name = "employees",
            CreatedMajor = createdMajor,
            ServerMajor = serverMajor,
            ServerVersion = $"{serverMajor}.0.0"
        };

        // Act
        IndexCompatibilityState state = compatibility.State;

        // Assert
        Assert.Equal(expected, state);
        Assert.Equal(expected is IndexCompatibilityState.RequiresReindex, compatibility.RequiresReindexBeforeNextMajorUpgrade);
    }

    [Theory]
    [InlineData("{\"acknowledged\":true,\"shards_acknowledged\":true,\"indices\":[{\"name\":\"employees\",\"blocked\":true}]}", true)]
    [InlineData("{\"acknowledged\":false,\"shards_acknowledged\":true,\"indices\":[{\"name\":\"employees\",\"blocked\":true}]}", false)]
    [InlineData("{\"acknowledged\":true,\"shards_acknowledged\":false,\"indices\":[{\"name\":\"employees\",\"blocked\":true}]}", false)]
    [InlineData("{\"acknowledged\":true,\"shards_acknowledged\":true,\"indices\":[{\"name\":\"employees\",\"blocked\":false}]}", false)]
    [InlineData("{\"acknowledged\":true,\"shards_acknowledged\":true,\"indices\":[{\"name\":\"other\",\"blocked\":true}]}", false)]
    [InlineData("{\"acknowledged\":true,\"shards_acknowledged\":true,\"indices\":[]}", false)]
    public void IsWriteBlockConfirmed_RequiresExactFullyAcknowledgedSource(string responseBody, bool expected)
    {
        // Arrange
        using var document = JsonDocument.Parse(responseBody);

        // Act
        bool confirmed = ElasticIndexCompatibilityUpgrader.IsWriteBlockConfirmed(document.RootElement, "employees");

        // Assert
        Assert.Equal(expected, confirmed);
    }

    [Theory]
    [InlineData("{\"nodes\":{},\"node_failures\":[{\"type\":\"failed_node_exception\"}]}")]
    [InlineData("{\"nodes\":{},\"task_failures\":[{\"task_id\":\"node:1\"}]}")]
    public void ParseActiveReindexTaskCount_WithPartialTaskListing_ReturnsUnknown(string responseBody)
    {
        // Arrange
        using var document = JsonDocument.Parse(responseBody);

        // Act
        int? count = ElasticIndexCompatibilityRecovery.ParseActiveReindexTaskCount(document.RootElement, "employees", "reindexed-v9-employees");

        // Assert
        Assert.Null(count);
    }

    [Fact]
    public void ShardsSucceeded_AcceptsUnassignedReplicasButRejectsFailures()
    {
        Assert.True(ElasticIndexCompatibilityUpgrader.ShardsSucceeded(new ShardStatistics { Failed = 0, Successful = 2, Total = 2 }));
        Assert.True(ElasticIndexCompatibilityUpgrader.ShardsSucceeded(new ShardStatistics { Failed = 0, Successful = 1, Total = 2 }));
        Assert.False(ElasticIndexCompatibilityUpgrader.ShardsSucceeded(new ShardStatistics { Failed = 1, Successful = 1, Total = 2 }));
        Assert.False(ElasticIndexCompatibilityUpgrader.ShardsSucceeded(new ShardStatistics { Failed = 0, Successful = 0, Total = 0 }));
        Assert.False(ElasticIndexCompatibilityUpgrader.ShardsSucceeded(null));
    }

    [Theory]
    [InlineData("employees", "employees")]
    [InlineData("employees-v1", "employees-v1")]
    [InlineData("reindexed-v8-employees", "employees")]
    [InlineData("reindexed-v9-employees-v1", "employees-v1")]
    [InlineData("reindexed-v8-logs-v1-2023.05.01", "logs-v1-2023.05.01")]
    [InlineData("reindexed-v-employees", "reindexed-v-employees")]
    [InlineData("reindexed-v0-employees", "reindexed-v0-employees")]
    public void CompatibilityIndexName_GetCanonicalName_ReturnsExpectedName(string name, string expected)
    {
        Assert.Equal(expected, CompatibilityIndexName.GetCanonicalName(name));
    }

    [Theory]
    [InlineData("employees", 8, "reindexed-v8-employees")]
    [InlineData("reindexed-v8-employees", 9, "reindexed-v9-employees")]
    [InlineData("reindexed-v9-employees-v1", 10, "reindexed-v10-employees-v1")]
    public void CompatibilityIndexName_Create_ReplacesExistingCompatibilityPrefix(string source, int serverMajor, string expected)
    {
        Assert.Equal(expected, CompatibilityIndexName.Create(source, serverMajor));
    }

    [Fact]
    public void CompatibilityIndexName_ConfiguredPrefixPreservesNaturalName()
    {
        // Arrange
        const string configuredName = "reindexed-v8-events";
        const string source = "reindexed-v8-events-v1";

        // Act
        string canonicalSource = CompatibilityIndexName.GetCanonicalName(source, configuredName);
        string target = CompatibilityIndexName.Create(source, 9, configuredName);
        string canonicalTarget = CompatibilityIndexName.GetCanonicalName(target, configuredName);

        // Assert
        Assert.Equal(source, canonicalSource);
        Assert.Equal("reindexed-v9-reindexed-v8-events-v1", target);
        Assert.Equal(source, canonicalTarget);
    }

    [Fact]
    public void CompatibilityIndexName_ConfiguredPrefixPreservesNaturalPlainName()
    {
        // Arrange
        const string configuredName = "reindexed-v8-events";

        // Act
        string canonicalName = CompatibilityIndexName.GetCanonicalName(configuredName, configuredName);
        string targetName = CompatibilityIndexName.Create(configuredName, 9, configuredName);

        // Assert
        Assert.Equal(configuredName, canonicalName);
        Assert.Equal("reindexed-v9-reindexed-v8-events", targetName);
    }

    [Theory]
    [InlineData("reindexed-v8-logs-2026.01.01", "reindexed-v8-logs-2026.01.01")]
    [InlineData("reindexed-v9-reindexed-v8-logs-2026.01.01", "reindexed-v8-logs-2026.01.01")]
    public void CompatibilityIndexName_ConfiguredPrefixPreservesNaturalChildName(string index, string expected)
    {
        // Arrange
        const string configuredName = "reindexed-v8-logs";

        // Act
        string canonicalName = CompatibilityIndexName.GetCanonicalName(index, configuredName);

        // Assert
        Assert.Equal(expected, canonicalName);
    }

    [Theory]
    [InlineData(null, "7.17.19", 7)]
    [InlineData(null, "8.11.0", 8)]
    [InlineData(null, "9.0.1", 9)]
    [InlineData(null, "9", 9)]
    [InlineData("7170199", null, 7)]
    [InlineData("8500003", null, 8)]
    [InlineData("9000000", null, 9)]
    [InlineData(null, null, null)]
    [InlineData("not-a-number", null, null)]
    [InlineData(null, "not-a-version", null)]
    [InlineData("7170199", "-1", 7)]
    [InlineData("9223372036854775807", null, null)]
    [InlineData("-1000000", null, null)]
    [InlineData("0", null, null)]
    public void ParseCreatedMajor_ParsesExpectedMajor(string? created, string? createdString, int? expectedMajor)
    {
        int? major = Foundatio.Repositories.Elasticsearch.Configuration.Index.ParseCreatedMajor(created, createdString);

        Assert.Equal(expectedMajor, major);
    }

    [Fact]
    public void ParseCreatedMajor_PrefersCreatedStringOverCreated()
    {
        // CreatedString ("8.x") should win even though the numeric Created id would parse to a different major.
        int? major = Foundatio.Repositories.Elasticsearch.Configuration.Index.ParseCreatedMajor("7170199", "8.11.0");

        Assert.Equal(8, major);
    }

    [Fact]
    public async Task GetIndexCompatibilityAsync_WithUnknownServerVersion_Throws()
    {
        using var configuration = new UnparseableVersionElasticConfiguration();
        using var index = new Index<object>(configuration, "employees");

        var exception = await Assert.ThrowsAsync<RepositoryException>(() => index.GetIndexCompatibilityAsync(TestContext.Current.CancellationToken));

        Assert.Contains("server version", exception.Message);
    }

    [Fact]
    public async Task UpgradeIndexCompatibilityAsync_RechecksCompatibilityInsideLock()
    {
        using var configuration = new ElasticConfiguration();
        using var index = new BecomesCompatibleIndex(configuration);

        await configuration.UpgradeIndexCompatibilityAsync([index], cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(2, index.CompatibilityChecks);
    }

    [Fact]
    public async Task UpgradeIndexCompatibilityAsync_WhenDetectionIsCanceled_PropagatesCancellation()
    {
        using var configuration = new ElasticConfiguration();
        using var index = new CanceledCompatibilityIndex(configuration);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => configuration.UpgradeIndexCompatibilityAsync([index], cancellationToken: TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task UpgradeIndexCompatibilityAsync_WithIndexFromDifferentConfiguration_ThrowsBeforeInspection()
    {
        using var owner = new ElasticConfiguration();
        using var other = new ElasticConfiguration();
        using var index = new CountingCompatibilityIndex(owner);

        var exception = await Assert.ThrowsAsync<ArgumentException>(() => other.UpgradeIndexCompatibilityAsync([index], cancellationToken: TestContext.Current.CancellationToken));

        Assert.Contains("different Elasticsearch configuration", exception.Message);
        Assert.Equal(0, index.CompatibilityChecks);
    }

    [Fact]
    public async Task UpgradeIndexCompatibilityAsync_WhenIndexSkippedMajor_ThrowsBeforeMutation()
    {
        using var configuration = new ElasticConfiguration();
        using var index = new UnsupportedCompatibilityIndex(configuration);

        var exception = await Assert.ThrowsAsync<RepositoryException>(() => configuration.UpgradeIndexCompatibilityAsync([index], cancellationToken: TestContext.Current.CancellationToken));

        Assert.Contains("one major at a time", exception.Message);
        Assert.Equal(1, index.CompatibilityChecks);
    }

    [Theory]
    [InlineData("employees*")]
    [InlineData("employees,other")]
    [InlineData("employees?")]
    public async Task InspectIndexCompatibilityUpgradeAsync_WithIndexExpression_RejectsBeforeRequest(string sourceIndex)
    {
        using var configuration = new ElasticConfiguration();
        using var index = new Index<object>(configuration, "employees");

        var exception = await Assert.ThrowsAsync<ArgumentException>(() =>
            configuration.InspectIndexCompatibilityUpgradeAsync(index, sourceIndex, TestContext.Current.CancellationToken));

        Assert.Contains("exact concrete source", exception.Message);
    }

    [Fact]
    public async Task InspectIndexCompatibilityUpgradeAsync_WithSourceOwnedByDifferentIndex_RejectsBeforeRequest()
    {
        using var configuration = new ElasticConfiguration();
        using var index = new Index<object>(configuration, "employees");

        var exception = await Assert.ThrowsAsync<ArgumentException>(() =>
            configuration.InspectIndexCompatibilityUpgradeAsync(index, "customers", TestContext.Current.CancellationToken));

        Assert.Contains("does not belong", exception.Message);
    }

    [Theory]
    [InlineData("employees", true)]
    [InlineData("reindexed-v9-employees", true)]
    [InlineData("customers", false)]
    public void OwnsCompatibilityIndex_ForPlainIndex_RequiresConfiguredCanonicalName(string sourceIndex, bool expected)
    {
        using var index = new Index<object>(new ElasticConfiguration(), "employees");

        Assert.Equal(expected, index.OwnsCompatibilityIndex(sourceIndex));
    }

    [Theory]
    [InlineData("employees-v1", true)]
    [InlineData("reindexed-v9-employees-v2", true)]
    [InlineData("employees-v1-other", false)]
    [InlineData("customers-v1", false)]
    public void OwnsCompatibilityIndex_ForVersionedIndex_RequiresExactOwnedVersion(string sourceIndex, bool expected)
    {
        using var index = new VersionedIndex<object>(new ElasticConfiguration(), "employees", 2);

        Assert.Equal(expected, index.OwnsCompatibilityIndex(sourceIndex));
    }

    [Fact]
    public void ValidateCompatibilityUpgradeSource_ForRetainedInactiveVersion_AllowsOlderSchema()
    {
        using var index = new VersionedIndex<object>(new ElasticConfiguration(), "employees", 2);

        index.ValidateCompatibilityUpgradeSource("employees-v1", ownsLogicalAlias: false);
    }

    [Fact]
    public void ValidateCompatibilityUpgradeSource_ForActiveOlderVersion_RequiresSchemaReindex()
    {
        using var index = new VersionedIndex<object>(new ElasticConfiguration(), "employees", 2);

        var exception = Assert.Throws<RepositoryException>(() => index.ValidateCompatibilityUpgradeSource("employees-v1", ownsLogicalAlias: true));

        Assert.Contains("schema reindex", exception.Message);
    }

    [Theory]
    [InlineData("logs-v1-2026.08.11", true)]
    [InlineData("reindexed-v9-logs-v2-2026.08.11", true)]
    [InlineData("logs-v1-not-a-date", false)]
    [InlineData("other-v1-2026.08.11", false)]
    public void OwnsCompatibilityIndex_ForDailyIndex_RequiresOwnedDatedPartition(string sourceIndex, bool expected)
    {
        using var index = new DailyIndex<object>(new ElasticConfiguration(), "logs", 2);

        Assert.Equal(expected, index.OwnsCompatibilityIndex(sourceIndex));
    }

    [Fact]
    public async Task ConfigureIndexesAsync_DoesNotCheckCompatibility()
    {
        using var configuration = new ElasticConfiguration();
        using var index = new CountingCompatibilityIndex(configuration);

        await configuration.ConfigureIndexesAsync([index]);

        Assert.Equal(0, index.CompatibilityChecks);
    }

    [Fact]
    public void GetCompatibilityIndexPattern_ForDailyIndex_IncludesAllPhysicalPartitions()
    {
        var index = new TestDailyIndex(new ElasticConfiguration(), "logs", 1);

        Assert.Equal("logs-v*-*,reindexed-v*-logs-v*-*", index.GetCompatibilityIndexPatternPublic());
    }

    [Fact]
    public void GetCompatibilityIndexPattern_ForVersionedIndex_IncludesRetainedPhysicalVersions()
    {
        // Arrange
        var index = new TestVersionedIndex(new ElasticConfiguration(), "employees", 3);

        // Act
        string pattern = index.GetCompatibilityIndexPatternPublic();

        // Assert
        Assert.Equal("employees-v*,reindexed-v*-employees-v*", pattern);
    }

    [Fact]
    public void DailyIndex_GetIndexDate_WithoutRevisionSuffix_ParsesDate()
    {
        var index = new TestDailyIndex(new ElasticConfiguration(), "logs", 1);

        var date = index.GetIndexDatePublic("logs-v1-2023.05.01");

        Assert.Equal(new DateTime(2023, 5, 1, 0, 0, 0, DateTimeKind.Utc), date);
    }

    [Fact]
    public void DailyIndex_GetIndexDate_WithCompatibilityPrefix_StripsPrefixAndParsesDate()
    {
        var index = new TestDailyIndex(new ElasticConfiguration(), "logs", 1);

        var date = index.GetIndexDatePublic("reindexed-v8-logs-v1-2023.05.01");

        Assert.Equal(new DateTime(2023, 5, 1, 0, 0, 0, DateTimeKind.Utc), date);
    }

    [Fact]
    public void DailyIndex_GetIndexDate_WithNewerCompatibilityPrefix_StripsPrefixAndParsesDate()
    {
        var index = new TestDailyIndex(new ElasticConfiguration(), "logs", 1);

        var date = index.GetIndexDatePublic("reindexed-v10-logs-v1-2023.05.01");

        Assert.Equal(new DateTime(2023, 5, 1, 0, 0, 0, DateTimeKind.Utc), date);
    }

    [Fact]
    public void VersionedIndex_GetIndexVersion_WithNaturalCompatibilityPrefixPreservesName()
    {
        var index = new TestVersionedIndex(new ElasticConfiguration(), "reindexed-v8-events", 1);

        Assert.Equal(1, index.GetIndexVersionPublic("reindexed-v8-events-v1"));
        Assert.Equal(1, index.GetIndexVersionPublic("reindexed-v9-reindexed-v8-events-v1"));
    }

    [Fact]
    public void DailyIndex_GetIndexDate_WithInvalidName_ReturnsMaxValue()
    {
        var index = new TestDailyIndex(new ElasticConfiguration(), "logs", 1);

        var date = index.GetIndexDatePublic("not-a-matching-name");

        Assert.Equal(DateTime.MaxValue, date);
    }

    private sealed class TestDailyIndex : Foundatio.Repositories.Elasticsearch.Configuration.DailyIndex
    {
        public TestDailyIndex(IElasticConfiguration configuration, string name, int version = 1) : base(configuration, name, version) { }

        public DateTime GetIndexDatePublic(string index) => GetIndexDate(index);

        public string GetCompatibilityIndexPatternPublic() => GetCompatibilityIndexPattern();
    }

    private sealed class TestVersionedIndex : VersionedIndex
    {
        public TestVersionedIndex(IElasticConfiguration configuration, string name, int version) : base(configuration, name, version) { }

        public int GetIndexVersionPublic(string name) => GetIndexVersion(name);

        public string GetCompatibilityIndexPatternPublic() => GetCompatibilityIndexPattern();
    }

    private sealed class BecomesCompatibleIndex : Index<object>
    {
        public BecomesCompatibleIndex(IElasticConfiguration configuration) : base(configuration, "becomes-compatible") { }

        public int CompatibilityChecks { get; private set; }

        public override Task<IReadOnlyCollection<IndexCompatibilityInfo>> GetIndexCompatibilityAsync(CancellationToken cancellationToken = default)
        {
            CompatibilityChecks++;
            IReadOnlyCollection<IndexCompatibilityInfo> result = CompatibilityChecks is 1
                ?
                [
                    new IndexCompatibilityInfo
                    {
                        Name = Name,
                        CreatedMajor = 8,
                        CreatedVersion = "8.0.0",
                        ServerMajor = 9,
                        ServerVersion = "9.0.0"
                    }
                ]
                : [];

            return Task.FromResult(result);
        }
    }

    private sealed class CanceledCompatibilityIndex : Index<object>
    {
        public CanceledCompatibilityIndex(IElasticConfiguration configuration) : base(configuration, "canceled-compatibility") { }

        public override Task<IReadOnlyCollection<IndexCompatibilityInfo>> GetIndexCompatibilityAsync(CancellationToken cancellationToken = default)
        {
            return Task.FromCanceled<IReadOnlyCollection<IndexCompatibilityInfo>>(new CancellationToken(true));
        }
    }

    private sealed class CountingCompatibilityIndex : Index<object>
    {
        public CountingCompatibilityIndex(IElasticConfiguration configuration) : base(configuration, "counting-compatibility") { }

        public int CompatibilityChecks { get; private set; }

        public override Task ConfigureAsync() => Task.CompletedTask;

        public override Task MaintainAsync(bool includeOptionalTasks = true) => Task.CompletedTask;

        public override Task<IReadOnlyCollection<IndexCompatibilityInfo>> GetIndexCompatibilityAsync(CancellationToken cancellationToken = default)
        {
            CompatibilityChecks++;
            return Task.FromResult<IReadOnlyCollection<IndexCompatibilityInfo>>([]);
        }
    }

    private sealed class UnsupportedCompatibilityIndex : Index<object>
    {
        public UnsupportedCompatibilityIndex(IElasticConfiguration configuration) : base(configuration, "unsupported-compatibility") { }

        public int CompatibilityChecks { get; private set; }

        public override Task<IReadOnlyCollection<IndexCompatibilityInfo>> GetIndexCompatibilityAsync(CancellationToken cancellationToken = default)
        {
            CompatibilityChecks++;
            return Task.FromResult<IReadOnlyCollection<IndexCompatibilityInfo>>(
            [
                new IndexCompatibilityInfo
                {
                    Name = Name,
                    CreatedMajor = 7,
                    CreatedVersion = "7.17.29",
                    ServerMajor = 9,
                    ServerVersion = "9.5.0"
                }
            ]);
        }
    }

    private sealed class UnparseableVersionElasticConfiguration : ElasticConfiguration
    {
        public int RequestCount { get; private set; }

        protected override ElasticsearchClient CreateElasticClient()
        {
            byte[] response = Encoding.UTF8.GetBytes("""
                {
                  "name": "test-node",
                  "cluster_name": "test-cluster",
                  "cluster_uuid": "test-cluster-id",
                  "version": {
                    "number": "not-a-version",
                    "build_flavor": "default",
                    "build_type": "unknown",
                    "build_hash": "unknown",
                    "build_date": "2026-01-01T00:00:00.000Z",
                    "build_snapshot": false,
                    "lucene_version": "10.0.0",
                    "minimum_wire_compatibility_version": "8.0.0",
                    "minimum_index_compatibility_version": "8.0.0"
                  },
                  "tagline": "You Know, for Search"
                }
                """);
            var headers = new Dictionary<string, IEnumerable<string>>
            {
                ["x-elastic-product"] = ["Elasticsearch"]
            };
            var requestInvoker = new InMemoryRequestInvoker(response, 200, null, "application/json", headers);
            var settings = new ElasticsearchClientSettings(requestInvoker)
                .OnRequestCompleted(_ => RequestCount++);

            return new ElasticsearchClient(settings);
        }
    }
}
