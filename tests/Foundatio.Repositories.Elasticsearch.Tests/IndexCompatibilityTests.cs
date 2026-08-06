using System;
using System.Collections.Generic;
using System.Text;
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
    public async Task ConfigureIndexesAsync_DoesNotCheckCompatibility()
    {
        using var configuration = new ElasticConfiguration();
        using var index = new CountingCompatibilityIndex(configuration);

        await configuration.ConfigureIndexesAsync([index]);

        Assert.Equal(0, index.CompatibilityChecks);
    }

    [Fact]
    public void GetCompatibilityIndexPattern_ForDailyIndex_UsesStableAlias()
    {
        var index = new TestDailyIndex(new ElasticConfiguration(), "logs", 1);

        Assert.Equal("logs", index.GetCompatibilityIndexPatternPublic());
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
                        ServerVersion = "9.0.0",
                        RequiresReindexBeforeNextMajorUpgrade = true
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
