using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Transport;
using Foundatio.Caching;
using Foundatio.Lock;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Exceptions;
using Foundatio.Utility;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public partial class IndexCompatibilityTests
{
    [Fact]
    public async Task ValidateAsync_WithDotPrefixedSource_RejectsBeforeMutation()
    {
        var requestInvoker = new SequenceRequestInvoker(
            new StubResponse(404, """{"error":{"type":"index_not_found_exception","reason":"no such index [reindexed-v9-.employees]"},"status":404}"""),
            new StubResponse(200, """{".employees":{"aliases":{},"mappings":{"_source":{"enabled":true}},"settings":{}}}"""),
            new StubResponse(200, """{".employees":{"settings":{}}}"""));
        var requestMethods = new List<string>();
        var settings = new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://localhost:9200")), requestInvoker)
            .OnRequestCompleted(call => requestMethods.Add(call.HttpMethod.ToString()));
        var client = new ElasticsearchClient(settings);
        var upgrader = new ElasticIndexCompatibilityUpgrader(client, TimeProvider.System);
        using var index = new Index<object>(new ElasticConfiguration(), ".employees");
        var compatibility = new IndexCompatibilityInfo
        {
            Name = index.Name,
            CreatedMajor = 8,
            ServerMajor = 9,
            ServerVersion = "9.0.0"
        };

        var exception = await Assert.ThrowsAsync<RepositoryException>(() =>
            upgrader.ValidateAsync(index, compatibility, CancellationToken.None));

        Assert.Contains("System or restricted index", exception.Message);
        Assert.Equal(["HEAD", "GET", "GET"], requestMethods);
    }

    [Fact]
    public void JsonDefinitionsMatch_IgnoresObjectPropertyOrder()
    {
        const string expected = """{"properties":{"name":{"type":"keyword","meta":{"first":"1","second":"2"}}}}""";
        const string actual = """{"properties":{"name":{"meta":{"second":"2","first":"1"},"type":"keyword"}}}""";

        Assert.True(ElasticIndexCompatibilityUpgrader.JsonDefinitionsMatch(expected, actual));
    }

    [Fact]
    public async Task UpgradeAsync_WhenCreateFromOutcomeIsUnknown_RetainsTargetAndFailsClosed()
    {
        // Arrange
        var headers = new Dictionary<string, IEnumerable<string>> { ["x-elastic-product"] = ["Elasticsearch"] };
        var requestInvoker = new SequenceRequestInvoker(
            new StubResponse(404, """{"error":{"type":"index_not_found_exception","reason":"no such index [reindexed-v9-employees]"},"status":404}"""),
            new StubResponse(200, """{"employees":{"aliases":{},"mappings":{"_source":{"enabled":true}},"settings":{}}}"""),
            new StubResponse(200, """{"employees":{"settings":{}}}"""),
            new StubResponse(200, """{"acknowledged":true}"""),
            new StubResponse(200, """{"acknowledged":true,"shards_acknowledged":true,"indices":[{"name":"employees","blocked":true}]}"""),
            new StubResponse(200, """{"_shards":{"total":1,"successful":1,"failed":0}}"""),
            new StubResponse(500, "", new TimeoutException("The _create_from response was not received.")));
        var client = new ElasticsearchClient(new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://localhost:9200")), requestInvoker));
        var upgrader = new ElasticIndexCompatibilityUpgrader(client, TimeProvider.System);
        using var index = new Index<object>(new ElasticConfiguration(), "employees");
        var compatibility = new IndexCompatibilityInfo
        {
            Name = "employees",
            CreatedMajor = 8,
            ServerMajor = 9,
            ServerVersion = "9.0.0"
        };
        var locks = new ThrottlingLockProvider(new InMemoryCacheClient());
        await using var reindexLock = await locks.AcquireAsync("compatibility-upgrade", cancellationToken: TestContext.Current.CancellationToken);

        // Act
        var exception = await Assert.ThrowsAsync<RepositoryException>(() =>
            upgrader.UpgradeAsync(index, compatibility, reindexLock!, (_, _) => Task.CompletedTask, CancellationToken.None));

        Assert.Contains("recovery evidence could not be inspected", exception.Message);
        Assert.Contains("_create_from response was not received", exception.ToString());
    }

    [Fact]
    public async Task UpgradeAsync_WhenTargetCleanupCannotBeConfirmed_KeepsSourceWriteBlocked()
    {
        // Arrange
        var requestInvoker = new SequenceRequestInvoker(
            new StubResponse(404, """{"error":{"type":"index_not_found_exception","reason":"no such index [reindexed-v9-employees]"},"status":404}"""),
            new StubResponse(200, """{"employees":{"aliases":{},"mappings":{"_source":{"enabled":true}},"settings":{}}}"""),
            new StubResponse(200, """{"employees":{"settings":{}}}"""),
            new StubResponse(200, """{"acknowledged":true}"""),
            new StubResponse(200, """{"acknowledged":true,"shards_acknowledged":true,"indices":[{"name":"employees","blocked":true}]}"""),
            new StubResponse(200, """{"_shards":{"total":1,"successful":1,"failed":0}}"""),
            new StubResponse(200, """{"acknowledged":true,"shards_acknowledged":true,"index":"reindexed-v9-employees"}"""),
            new StubResponse(200, """{"acknowledged":true}"""),
            new StubResponse(200, """{"reindexed-v9-employees":{"aliases":{"unexpected":{}},"mappings":{"_source":{"enabled":true}},"settings":{}}}"""),
            new StubResponse(200, """{"reindexed-v9-employees":{"settings":{}}}"""),
            new StubResponse(500, """{"error":{"type":"master_not_discovered_exception","reason":"delete outcome unknown"},"status":500}"""),
            new StubResponse(200, """{"acknowledged":true}"""));
        var requestPaths = new List<string>();
        var settings = new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://localhost:9200")), requestInvoker)
            .OnRequestCompleted(call => requestPaths.Add($"{call.HttpMethod} {call.Uri?.AbsolutePath}"));
        var client = new ElasticsearchClient(settings);
        var upgrader = new ElasticIndexCompatibilityUpgrader(client, TimeProvider.System);
        using var index = new Index<object>(new ElasticConfiguration(), "employees");
        var compatibility = new IndexCompatibilityInfo
        {
            Name = "employees",
            CreatedMajor = 8,
            ServerMajor = 9,
            ServerVersion = "9.0.0"
        };
        var locks = new ThrottlingLockProvider(new InMemoryCacheClient());
        await using var reindexLock = await locks.AcquireAsync("compatibility-upgrade", cancellationToken: TestContext.Current.CancellationToken);

        // Act
        await Assert.ThrowsAsync<RepositoryException>(() =>
            upgrader.UpgradeAsync(index, compatibility, reindexLock!, (_, _) => Task.CompletedTask, CancellationToken.None));

        // Assert: exposing the source to writes is unsafe while the destination may still exist.
        Assert.DoesNotContain("PUT /employees/_settings", requestPaths);
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
        var upgrader = new ElasticIndexCompatibilityUpgrader(client, TimeProvider.System);
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
        // Act
        bool confirmed = ElasticIndexCompatibilityUpgrader.IsWriteBlockConfirmed(responseBody, "employees");

        // Assert
        Assert.Equal(expected, confirmed);
    }

    [Fact]
    public async Task PutSettings_WithFlatOtherSettings_SerializesExplicitNullResets()
    {
        var headers = new Dictionary<string, IEnumerable<string>> { ["x-elastic-product"] = ["Elasticsearch"] };
        var requestInvoker = new InMemoryRequestInvoker(
            Encoding.UTF8.GetBytes("{\"acknowledged\":true}"), 200, null, "application/json", headers);
        byte[]? requestBody = null;
        var settings = new ElasticsearchClientSettings(requestInvoker)
            .DisableDirectStreaming()
            .OnRequestCompleted(call => requestBody = call.RequestBodyInBytes);
        var client = new ElasticsearchClient(settings);
        var nullValue = JsonSerializer.SerializeToElement<object?>(null);

        var response = await client.Indices.PutSettingsAsync("employees", d => d.Settings(new IndexSettings
        {
            OtherSettings = new Dictionary<string, object>
            {
                ["index.refresh_interval"] = nullValue,
                ["index.default_pipeline"] = nullValue,
                ["index.final_pipeline"] = nullValue
            }
        }), TestContext.Current.CancellationToken);

        Assert.True(response.IsValidResponse, response.GetErrorMessage());
        Assert.NotNull(requestBody);
        using var document = JsonDocument.Parse(requestBody);
        Assert.Equal(JsonValueKind.Null, document.RootElement.GetProperty("index.refresh_interval").ValueKind);
        Assert.Equal(JsonValueKind.Null, document.RootElement.GetProperty("index.default_pipeline").ValueKind);
        Assert.Equal(JsonValueKind.Null, document.RootElement.GetProperty("index.final_pipeline").ValueKind);
    }

    [Fact]
    public async Task GetSettings_WithFlatSettings_PreservesExplicitSettingPaths()
    {
        var headers = new Dictionary<string, IEnumerable<string>> { ["x-elastic-product"] = ["Elasticsearch"] };
        var requestInvoker = new InMemoryRequestInvoker(
            Encoding.UTF8.GetBytes("""
                {
                  "employees": {
                    "settings": {
                      "index.max_result_window": "12345",
                      "index.number_of_replicas": "1"
                    }
                  }
                }
                """), 200, null, "application/json", headers);
        var client = new ElasticsearchClient(new ElasticsearchClientSettings(requestInvoker));

        var response = await client.Indices.GetSettingsAsync((Indices)"employees",
            d => d.FlatSettings().IncludeDefaults(false), TestContext.Current.CancellationToken);

        Assert.True(response.IsValidResponse, response.GetErrorMessage());
        var state = response.RequireSingleResolvedIndexState("employees");
        var settings = state.Settings?.Index ?? state.Settings;
        Assert.NotNull(settings?.OtherSettings);
        Assert.Equal("12345", settings.OtherSettings["index.max_result_window"].ToString());
        Assert.Equal("1", settings.OtherSettings["index.number_of_replicas"].ToString());
    }

    [Fact]
    public async Task GetCurrentVersionAsync_WithHiddenDatedAlias_UsesAliasFromIndexMetadata()
    {
        const string name = "hidden-logs";
        string date = DateTime.UtcNow.ToString("yyyy.MM.dd");
        string responseBody = $$"""
            {
              "{{name}}-v1-{{date}}": { "aliases": {} },
              "{{name}}-v2-{{date}}": {
                "aliases": {
                  "{{name}}-{{date}}": { "is_hidden": true }
                }
              }
            }
            """;
        var requestInvoker = new SequenceRequestInvoker(
            new StubResponse(200, responseBody),
            new StubResponse(200, "{}"));
        using var configuration = new RequestInvokerElasticConfiguration(requestInvoker);
        using var index = new TestDailyIndex(configuration, name, 3);

        int currentVersion = await index.GetCurrentVersionAsync();

        Assert.Equal(2, currentVersion);
        Assert.Equal(1, configuration.RequestCount);
    }

    [Fact]
    public void IsCompletedCutover_RequiresExactCleanCommittedTopology()
    {
        var completed = new IndexCompatibilityUpgradeStatus
        {
            IndexName = "employees",
            SourceIndex = "employees-v1",
            TargetIndex = "reindexed-v9-employees-v1",
            Action = IndexCompatibilityRecoveryAction.None,
            SourceExists = false,
            TargetExists = true,
            TargetHasCanonicalSourceAlias = true,
            ActiveReindexTaskCount = 0
        };

        Assert.True(ElasticIndexCompatibilityUpgrader.IsCompletedCutover(completed));
        Assert.False(ElasticIndexCompatibilityUpgrader.IsCompletedCutover(completed with { SourceExists = true }));
        Assert.False(ElasticIndexCompatibilityUpgrader.IsCompletedCutover(completed with { TargetWorkflowMarkerPresent = true }));
        Assert.False(ElasticIndexCompatibilityUpgrader.IsCompletedCutover(completed with { TargetWriteBlocked = true }));
        Assert.False(ElasticIndexCompatibilityUpgrader.IsCompletedCutover(completed with { TargetHasCanonicalSourceAlias = false }));
        Assert.False(ElasticIndexCompatibilityUpgrader.IsCompletedCutover(completed with { ActiveReindexTaskCount = null }));
        Assert.False(ElasticIndexCompatibilityUpgrader.IsCompletedCutover(completed with { Action = IndexCompatibilityRecoveryAction.ManualIntervention }));
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
    [InlineData("reindexed-v08-employees", "reindexed-v08-employees")]
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

    [Fact]
    public void CompatibilityIndexName_ConfiguredReindexedName_StripsGeneratedWrapper()
    {
        const string configuredName = "reindexed";
        const string source = "reindexed-v1";
        const string target = "reindexed-v9-reindexed-v1";

        Assert.Equal(source, CompatibilityIndexName.GetCanonicalName(source, configuredName));
        Assert.Equal(target, CompatibilityIndexName.Create(source, 9, configuredName));
        Assert.Equal(source, CompatibilityIndexName.GetCanonicalName(target, configuredName));
        Assert.Equal("reindexed-v10-reindexed-v1", CompatibilityIndexName.Create(target, 10, configuredName));
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
        configuration.AddIndex(index);
        await index.ConfigureAsync();
        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());

        await configuration.UpgradeIndexCompatibilityAsync([index], cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(2, index.CompatibilityChecks);
    }

    [Fact]
    public async Task UpgradeIndexCompatibilityAsync_WhenSourcesShareDestination_ThrowsBeforeMutation()
    {
        using var configuration = new ElasticConfiguration();
        using var index = new ConflictingDestinationIndex(configuration);
        configuration.AddIndex(index);

        var exception = await Assert.ThrowsAsync<RepositoryException>(() =>
            configuration.UpgradeIndexCompatibilityAsync([index], cancellationToken: TestContext.Current.CancellationToken));

        Assert.Contains("reindexed-v9-conflicting-destination-v1", exception.Message);
        Assert.Contains("conflicting-destination-v1", exception.Message);
        Assert.Contains("reindexed-v8-conflicting-destination-v1", exception.Message);
        Assert.Equal(1, index.CompatibilityChecks);
    }

    [Fact]
    public async Task UpgradeIndexCompatibilityAsync_WhenDetectionIsCanceled_PropagatesCancellation()
    {
        using var configuration = new ElasticConfiguration();
        using var index = new CanceledCompatibilityIndex(configuration);
        configuration.AddIndex(index);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => configuration.UpgradeIndexCompatibilityAsync([index], cancellationToken: TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task UpgradeIndexCompatibilityAsync_WithIndexFromDifferentConfiguration_ThrowsBeforeInspection()
    {
        using var owner = new ElasticConfiguration();
        using var other = new ElasticConfiguration();
        using var index = new CountingCompatibilityIndex(owner);
        owner.AddIndex(index);

        var exception = await Assert.ThrowsAsync<ArgumentException>(() => other.UpgradeIndexCompatibilityAsync([index], cancellationToken: TestContext.Current.CancellationToken));

        Assert.Contains("different Elasticsearch configuration", exception.Message);
        Assert.Equal(0, index.CompatibilityChecks);
    }

    [Fact]
    public async Task UpgradeIndexCompatibilityAsync_WhenRegisteredIndexesClaimSameSource_RejectsWholeBatchBeforeMutation()
    {
        using var configuration = new ElasticConfiguration();
        using var first = new StaticCompatibilityIndex(configuration, "employees", "shared-v1");
        using var second = new StaticCompatibilityIndex(configuration, "employees-archive", "shared-v1");
        configuration.AddIndex(first);
        configuration.AddIndex(second);

        var exception = await Assert.ThrowsAsync<RepositoryException>(() =>
            configuration.UpgradeIndexCompatibilityAsync([first, second], cancellationToken: TestContext.Current.CancellationToken));

        Assert.Contains("same compatibility source", exception.Message);
        Assert.Contains("shared-v1", exception.Message);
    }

    [Fact]
    public async Task UpgradeIndexCompatibilityAsync_WithLaterForeignIndex_ValidatesEntireBatchBeforeInspection()
    {
        using var configuration = new ElasticConfiguration();
        using var other = new ElasticConfiguration();
        using var first = new CountingCompatibilityIndex(configuration);
        using var foreign = new CountingCompatibilityIndex(other);
        configuration.AddIndex(first);
        other.AddIndex(foreign);

        var exception = await Assert.ThrowsAsync<ArgumentException>(() =>
            configuration.UpgradeIndexCompatibilityAsync([first, foreign], cancellationToken: TestContext.Current.CancellationToken));

        Assert.Contains("different Elasticsearch configuration", exception.Message);
        Assert.Equal(0, first.CompatibilityChecks);
        Assert.Equal(0, foreign.CompatibilityChecks);
    }

    [Fact]
    public async Task UpgradeIndexCompatibilityAsync_WithUnregisteredIndex_ThrowsBeforeInspection()
    {
        using var configuration = new ElasticConfiguration();
        using var index = new CountingCompatibilityIndex(configuration);

        var exception = await Assert.ThrowsAsync<ArgumentException>(() =>
            configuration.UpgradeIndexCompatibilityAsync([index], cancellationToken: TestContext.Current.CancellationToken));

        Assert.Contains("registered", exception.Message);
        Assert.Equal(0, index.CompatibilityChecks);
    }

    [Fact]
    public async Task UpgradeIndexCompatibilityAsync_WhenIndexSkippedMajor_ThrowsBeforeMutation()
    {
        using var configuration = new ElasticConfiguration();
        using var index = new UnsupportedCompatibilityIndex(configuration);
        configuration.AddIndex(index);

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
    [InlineData(false)]
    [InlineData(true)]
    public async Task CompatibilityRecovery_WithSourceOwnedByRegisteredSibling_RejectsBeforeRequest(bool recover)
    {
        using var configuration = new ElasticConfiguration();
        using var events = new VersionedIndex<object>(configuration, "events", 1);
        using var natural = new VersionedIndex<object>(configuration, "reindexed-v8-events", 1);
        configuration.AddIndex(events);
        configuration.AddIndex(natural);

        var exception = recover
            ? await Assert.ThrowsAsync<ArgumentException>(() => configuration.RecoverIndexCompatibilityUpgradeAsync(
                events,
                natural.VersionedName,
                TestContext.Current.CancellationToken))
            : await Assert.ThrowsAsync<ArgumentException>(() => configuration.InspectIndexCompatibilityUpgradeAsync(
                events,
                natural.VersionedName,
                TestContext.Current.CancellationToken));

        Assert.Contains("does not belong", exception.Message);
    }

    [Fact]
    public void ValidateDistinctSourceAndTarget_WithCurrentMajorDestination_RejectsRecovery()
    {
        var exception = Assert.Throws<ArgumentException>(() =>
            ElasticIndexCompatibilityRecovery.ValidateDistinctSourceAndTarget("reindexed-v9-employees", "reindexed-v9-employees"));

        Assert.Contains("original pre-upgrade", exception.Message);
    }

    [Theory]
    [InlineData("employees", true)]
    [InlineData("employees-error", true)]
    [InlineData("reindexed-v9-employees", true)]
    [InlineData("customers", false)]
    [InlineData("employees-error2", false)]
    public void MatchesCompatibilitySource_ForPlainIndex_RequiresConfiguredCanonicalName(string sourceIndex, bool expected)
    {
        using var index = new Index<object>(new ElasticConfiguration(), "employees");
        IReadOnlyDictionary<string, Alias> aliases = sourceIndex.StartsWith("reindexed-v", StringComparison.Ordinal)
            ? new Dictionary<string, Alias> { ["employees"] = new() }
            : sourceIndex.EndsWith("-error", StringComparison.Ordinal)
                ? new Dictionary<string, Alias> { [ElasticReindexer.ErrorIndexOwnershipAlias] = new() { IsHidden = true } }
                : new Dictionary<string, Alias>();

        Assert.Equal(expected, index.MatchesCompatibilitySource(sourceIndex, aliases));
    }

    [Theory]
    [InlineData("employees-v1", true)]
    [InlineData("employees-v1-error", true)]
    [InlineData("reindexed-v9-employees-v2", true)]
    [InlineData("reindexed-v9-employees-v2-error", true)]
    [InlineData("employees-v1-other", false)]
    [InlineData("employees-v1-errors", false)]
    [InlineData("customers-v1", false)]
    public void MatchesCompatibilitySource_ForVersionedIndex_RequiresExactOwnedVersion(string sourceIndex, bool expected)
    {
        using var index = new VersionedIndex<object>(new ElasticConfiguration(), "employees", 2);
        string canonical = CompatibilityIndexName.GetCanonicalName(sourceIndex, index.Name);
        var aliases = new Dictionary<string, Alias>();
        if (!String.Equals(canonical, sourceIndex, StringComparison.Ordinal))
            aliases[canonical] = new();
        if (canonical.EndsWith("-error", StringComparison.Ordinal))
            aliases[ElasticReindexer.ErrorIndexOwnershipAlias] = new() { IsHidden = true };

        Assert.Equal(expected, index.MatchesCompatibilitySource(sourceIndex, aliases));
    }

    [Fact]
    public void ValidateCompatibilityUpgradeSource_ForRetainedInactiveVersion_AllowsOlderSchema()
    {
        using var index = new VersionedIndex<object>(new ElasticConfiguration(), "employees", 2);

        index.ValidateCompatibilityUpgradeSource("employees-v1", new Dictionary<string, Alias>());
    }

    [Fact]
    public void ValidateCompatibilityUpgradeSource_ForActiveOlderVersion_RequiresSchemaReindex()
    {
        using var index = new VersionedIndex<object>(new ElasticConfiguration(), "employees", 2);

        var exception = Assert.Throws<RepositoryException>(() => index.ValidateCompatibilityUpgradeSource(
            "employees-v1",
            new Dictionary<string, Alias> { ["employees"] = new() }));

        Assert.Contains("schema reindex", exception.Message);
    }

    [Theory]
    [InlineData("logs-v1-2026.08.11", true)]
    [InlineData("logs-v1-2026.08.11-error", true)]
    [InlineData("reindexed-v9-logs-v2-2026.08.11", true)]
    [InlineData("logs-v1-not-a-date", false)]
    [InlineData("logs-v1-not-a-date-error", false)]
    [InlineData("other-v1-2026.08.11", false)]
    public void MatchesCompatibilitySource_ForDailyIndex_RequiresOwnedDatedPartition(string sourceIndex, bool expected)
    {
        using var index = new DailyIndex<object>(new ElasticConfiguration(), "logs", 2);
        string canonical = CompatibilityIndexName.GetCanonicalName(sourceIndex, index.Name);
        var aliases = new Dictionary<string, Alias>();
        if (!String.Equals(canonical, sourceIndex, StringComparison.Ordinal))
            aliases[canonical] = new();
        if (canonical.EndsWith("-error", StringComparison.Ordinal))
            aliases[ElasticReindexer.ErrorIndexOwnershipAlias] = new() { IsHidden = true };

        Assert.Equal(expected, index.MatchesCompatibilitySource(sourceIndex, aliases));
    }

    [Fact]
    public void MatchesCompatibilitySource_WhenSiblingNativelyClaimsName_YieldsToSibling()
    {
        // Arrange
        using var configuration = new ElasticConfiguration();
        using var events = new VersionedIndex<object>(configuration, "events", 1);
        using var natural = new VersionedIndex<object>(configuration, "reindexed-v8-events", 1);
        configuration.AddIndex(events);
        configuration.AddIndex(natural);

        // Assert
        Assert.False(events.MatchesCompatibilitySource(natural.VersionedName, new Dictionary<string, Alias>()));
        Assert.True(natural.MatchesCompatibilitySource(natural.VersionedName, new Dictionary<string, Alias>()));
        const string repeatedlyWrappedSibling = "reindexed-v9-reindexed-v8-events-v1";
        var siblingAliases = new Dictionary<string, Alias> { [natural.VersionedName] = new() };
        Assert.False(events.MatchesCompatibilitySource(repeatedlyWrappedSibling, siblingAliases));
        Assert.True(natural.MatchesCompatibilitySource(repeatedlyWrappedSibling, siblingAliases));
        Assert.True(events.MatchesCompatibilitySource(events.VersionedName, new Dictionary<string, Alias>()));
        Assert.False(natural.MatchesCompatibilitySource(events.VersionedName, new Dictionary<string, Alias>()));
    }

    [Fact]
    public void MatchesCompatibilitySource_WhenAnotherInstanceUsesSameLogicalName_KeepsOwnership()
    {
        using var configuration = new ElasticConfiguration();
        using var registered = new VersionedIndex<object>(configuration, "events", 1);
        using var adHoc = new VersionedIndex<object>(configuration, "events", 1);
        configuration.AddIndex(registered);

        Assert.True(adHoc.MatchesCompatibilitySource(adHoc.VersionedName, new Dictionary<string, Alias>()));
    }

}
