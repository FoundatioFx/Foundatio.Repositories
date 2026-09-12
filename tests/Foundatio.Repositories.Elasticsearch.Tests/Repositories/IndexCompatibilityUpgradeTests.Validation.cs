using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Clients.Elasticsearch.Mapping;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Exceptions;
using Foundatio.Utility;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed partial class IndexCompatibilityUpgradeTests
{
    [Fact]
    public async Task UpgradeIndexCompatibilityAsync_WhenDestinationAliasesChangeBeforeCutover_FailsBeforeDeletingSource()
    {
        // Arrange
        string name = $"compat-target-alias-change-{Guid.NewGuid():N}";
        string unexpectedAlias = $"{name}-unexpected";
        var index = new ForcedIncompatibleEmployeeIndex(_configuration, name);
        await index.DeleteAsync();
        await index.ConfigureAsync();
        var repository = new EmployeeRepository(index);
        await repository.AddAsync(EmployeeGenerator.Generate(), o => o.ImmediateConsistency());
        var compatibility = Assert.Single(await index.GetIndexCompatibilityAsync(TestCancellationToken));
        string targetIndex = CompatibilityIndexName.Create(name, compatibility.ServerMajor);
        await using AsyncDisposableAction _ = new(async () =>
            await _client.Indices.DeleteAsync(Indices.Parse($"{name},{targetIndex}"), d => d.IgnoreUnavailable(), TestCancellationToken));
        bool aliasAdded = false;
        RegisterCompatibilityIndex(index);

        // Act: inject an unexpected alias on the target mid-upgrade so the cutover safety check must reject it
        var exception = await Assert.ThrowsAsync<RepositoryException>(() => _configuration.UpgradeIndexCompatibilityAsync(
            [index],
            async (progress, message) =>
            {
                if (progress is not 92 || aliasAdded || message?.Contains("restored index settings", StringComparison.Ordinal) is not true)
                    return;

                var aliasResponse = await _client.Indices.UpdateAliasesAsync(a => a.Actions(actions => actions.Add(add => add
                    .Index(targetIndex)
                    .Alias(unexpectedAlias))), TestCancellationToken);
                Assert.True(aliasResponse.IsValidResponse, aliasResponse.GetErrorMessage());
                aliasAdded = true;
            },
            TestCancellationToken));

        // Assert
        Assert.True(aliasAdded);
        Assert.Contains("unexpected aliases before cutover", exception.ToString());
        await AssertIndexExistsAsync(name, true);
        await AssertIndexExistsAsync(targetIndex, true);
        var status = await _configuration.InspectIndexCompatibilityUpgradeAsync(index, name, TestCancellationToken);
        Assert.Equal(IndexCompatibilityRecoveryAction.ManualIntervention, status.Action);
        Assert.True(status.SourceWriteBlocked);
        Assert.True(status.TargetWriteBlocked);
    }

    [Theory]
    [InlineData("includes")]
    [InlineData("excludes")]
    [InlineData("long-name")]
    public async Task UpgradeIndexCompatibilityAsync_WithInvalidLaterSource_PreservesEntireBatch(string invalidSource)
    {
        // Arrange: one valid index and one index whose mapping is invalid for the upgrade (unsupported
        // _source filtering, or a name too long for the versioned target)
        string validName = $"compat-batch-valid-{Guid.NewGuid():N}";
        string invalidName = $"compat-batch-invalid-{Guid.NewGuid():N}";
        if (invalidSource is "long-name")
            invalidName = invalidName.PadRight(246, 'a');
        using var validIndex = new ForcedIncompatibleEmployeeIndex(_configuration, validName);
        using var invalidIndex = new ForcedIncompatibleEmployeeIndex(_configuration, invalidName);
        RegisterCompatibilityIndex(validIndex);
        RegisterCompatibilityIndex(invalidIndex);
        await validIndex.ConfigureAsync();
        var create = await _client.Indices.CreateAsync(new CreateIndexRequest(invalidName)
        {
            Settings = new IndexSettings { NumberOfShards = 1, NumberOfReplicas = 0 },
            Mappings = new TypeMapping
            {
                Source = new SourceField
                {
                    Includes = invalidSource is "includes" ? ["visible"] : null,
                    Excludes = invalidSource is "excludes" ? ["secret"] : null
                },
                Properties = new Properties
                {
                    ["visible"] = new KeywordProperty(),
                    ["secret"] = new KeywordProperty { Store = true }
                }
            }
        }, TestCancellationToken);
        Assert.True(create.IsValidResponse, create.GetErrorMessage());
        await using AsyncDisposableAction _ = new(async () =>
            await _client.Indices.DeleteAsync(Indices.Parse($"{validName},{invalidName}"), d => d.IgnoreUnavailable(), TestCancellationToken));
        var document = await _client.IndexAsync(new { visible = "keep", secret = "indexed-value" }, d => d.Index(invalidName).Id("1").Refresh(Refresh.True), TestCancellationToken);
        Assert.True(document.IsValidResponse, document.GetErrorMessage());

        // Act: upgrading the batch must fail entirely rather than upgrade the valid index and strand the invalid one
        var exception = await Assert.ThrowsAsync<RepositoryException>(() =>
            _configuration.UpgradeIndexCompatibilityAsync([validIndex, invalidIndex], cancellationToken: TestCancellationToken));

        // Assert
        Assert.Contains(invalidSource is "long-name" ? "255" : "_source", exception.Message);
        var states = await _client.Indices.GetAsync(Indices.Parse($"{validName},{invalidName}"), cancellationToken: TestCancellationToken);
        Assert.True(states.IsValidResponse, states.GetErrorMessage());
        Assert.Equal(2, states.Indices.Count);
        foreach (var state in states.Indices.Values)
        {
            Assert.False(state.Settings?.Index?.Blocks?.Write is true);
            Assert.DoesNotContain(ElasticIndexCompatibilityUpgrader.OwnershipAlias, state.Aliases!.Keys);
        }
        var count = await _client.CountAsync<object>(d => d.Indices(invalidName).Query(q => q.Term(t => t.Field("secret").Value("indexed-value"))), TestCancellationToken);
        Assert.True(count.IsValidResponse, count.GetErrorMessage());
        Assert.Equal(1, count.Count);
        int major = Assert.Single(await validIndex.GetIndexCompatibilityAsync(TestCancellationToken)).ServerMajor;
        await AssertIndexExistsAsync($"reindexed-v{major}-{validName}", false);
        await AssertIndexExistsAsync($"reindexed-v{major}-{invalidName}", false);
    }

    [Fact]
    public async Task UpgradeIndexCompatibilityAsync_WithPendingDailySchemaUpgrade_ThrowsBeforeChanges()
    {
        // Arrange
        string name = $"compat-schema-precedence-{Guid.NewGuid():N}";
        var currentIndex = new DailyIndex<Employee>(_configuration, name, 1);
        await using AsyncDisposableAction _ = new(() => currentIndex.DeleteAsync());
        var repository = new EmployeeRepository(currentIndex);
        await repository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());
        using var nextIndex = new ForcedIncompatibleDailyEmployeeIndex(_configuration, name, 2);
        var compatibility = Assert.Single(await nextIndex.GetIndexCompatibilityAsync(TestCancellationToken));
        RegisterCompatibilityIndex(nextIndex);

        // Act: a daily schema reindex is still pending, so the compatibility upgrade must refuse to proceed
        var exception = await Assert.ThrowsAsync<RepositoryException>(() => _configuration.UpgradeIndexCompatibilityAsync([nextIndex], cancellationToken: TestCancellationToken));

        // Assert
        Assert.Contains("schema reindex", exception.Message);
        var sourceResponse = await _client.Indices.GetAsync((Indices)compatibility.Name, cancellationToken: TestCancellationToken);
        Assert.True(sourceResponse.IsValidResponse, sourceResponse.GetErrorMessage());
        Assert.Contains(compatibility.Name, sourceResponse.Indices.Keys.Select(k => k.ToString()));
        var settings = sourceResponse.Indices.Values.Single().Settings?.Index;
        Assert.False(settings?.Blocks?.Write is true);
    }

    [Fact]
    public async Task UpgradeIndexCompatibilityAsync_WithPendingSchemaUpgradeAndMissingLogicalAlias_ThrowsBeforeChanges()
    {
        // Arrange
        string name = $"compat-schema-no-alias-{Guid.NewGuid():N}";
        var version1 = new ForcedIncompatibleVersionedEmployeeIndex(_configuration, name, 1);
        await using AsyncDisposableAction _ = new(() => version1.DeleteAsync());
        await version1.ConfigureAsync();
        var removeAlias = await _client.Indices.UpdateAliasesAsync(a => a.Actions(action => action.Remove(remove => remove
            .Index(version1.VersionedName)
            .Alias(name))), TestCancellationToken);
        Assert.True(removeAlias.IsValidResponse, removeAlias.GetErrorMessage());
        using var version2 = new ForcedIncompatibleVersionedEmployeeIndex(_configuration, name, 2);
        var compatibility = Assert.Single(await version2.GetIndexCompatibilityAsync(TestCancellationToken));
        RegisterCompatibilityIndex(version2);

        // Act: the logical alias is missing, so the pending schema reindex cannot be detected through it either
        var exception = await Assert.ThrowsAsync<RepositoryException>(() =>
            _configuration.UpgradeIndexCompatibilityAsync([version2], cancellationToken: TestCancellationToken));

        // Assert
        Assert.Contains("schema reindex", exception.Message);
        await AssertIndexExistsAsync(compatibility.Name, true);
        await AssertIndexExistsAsync(CompatibilityIndexName.Create(compatibility.Name, compatibility.ServerMajor, name), false);
    }

    [Fact]
    public async Task UpgradeIndexCompatibilityAsync_WhenSourceBecomesActiveAfterPreflight_RechecksSchemaBeforeChanges()
    {
        // Arrange
        string name = $"compat-schema-execution-{Guid.NewGuid():N}";
        var version1 = new ForcedIncompatibleVersionedEmployeeIndex(_configuration, name, 1);
        var version2 = new ForcedIncompatibleVersionedEmployeeIndex(_configuration, name, 2);
        await using AsyncDisposableAction _ = new(() => version2.DeleteAsync());
        await version1.ConfigureAsync();
        await version2.ConfigureAsync();
        var activateVersion2 = await _client.Indices.UpdateAliasesAsync(a => a.Actions(
            action => action.Remove(remove => remove.Index(version1.VersionedName).Alias(name)),
            action => action.Add(add => add.Index(version2.VersionedName).Alias(name))),
            cancellationToken: TestCancellationToken);
        Assert.True(activateVersion2.IsValidResponse, activateVersion2.GetErrorMessage());
        var version1Compatibility = Assert.Single(
            await version2.GetIndexCompatibilityAsync(TestCancellationToken),
            info => String.Equals(info.Name, version1.VersionedName, StringComparison.Ordinal));
        bool sourceBecameActive = false;
        RegisterCompatibilityIndex(version2);

        // Act: swap the active version back to version1 mid-upgrade so the pending schema reindex reappears
        var exception = await Assert.ThrowsAsync<RepositoryException>(() => _configuration.UpgradeIndexCompatibilityAsync(
            [version2],
            async (progress, _) =>
            {
                if (progress is not 0 || sourceBecameActive)
                    return;

                sourceBecameActive = true;
                var activateVersion1 = await _client.Indices.UpdateAliasesAsync(a => a.Actions(
                    action => action.Remove(remove => remove.Index(version2.VersionedName).Alias(name)),
                    action => action.Add(add => add.Index(version1.VersionedName).Alias(name))),
                    cancellationToken: TestCancellationToken);
                Assert.True(activateVersion1.IsValidResponse, activateVersion1.GetErrorMessage());
            },
            TestCancellationToken));

        // Assert
        Assert.True(sourceBecameActive);
        Assert.Contains("schema reindex", exception.Message);
        Assert.False((await _client.Indices.ExistsAsync(
            CompatibilityIndexName.Create(version1.VersionedName, version1Compatibility.ServerMajor, name),
            cancellationToken: TestCancellationToken)).Exists);
    }

    [Fact]
    public async Task UpgradeIndexCompatibilityAsync_WhenCompatibilityRemains_Throws()
    {
        // Arrange
        var index = new AlwaysIncompatibleEmployeeIndex(_configuration, $"compat-remains-{Guid.NewGuid():N}");
        await index.DeleteAsync();
        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());
        await index.ConfigureAsync();
        RegisterCompatibilityIndex(index);

        // Act: the index remains incompatible after the upgrade, so it must be reported as failed
        var exception = await Assert.ThrowsAsync<RepositoryException>(() => _configuration.UpgradeIndexCompatibilityAsync([index], cancellationToken: TestCancellationToken));

        // Assert
        Assert.Contains("did not complete", exception.Message);
    }

    [Fact]
    public async Task UpgradeIndexCompatibilityAsync_WhenDestinationExists_ThrowsBeforeReindex()
    {
        // Arrange
        string name = $"compat-collision-{Guid.NewGuid():N}";
        var index = new ForcedIncompatibleEmployeeIndex(_configuration, name);
        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());
        await index.ConfigureAsync();
        var compatibility = Assert.Single(await index.GetIndexCompatibilityAsync(TestCancellationToken));
        string destination = CompatibilityIndexName.Create(name, compatibility.ServerMajor);
        await using AsyncDisposableAction destinationScope = new(async () =>
            await _client.Indices.DeleteAsync(destination, d => d.IgnoreUnavailable(), TestCancellationToken));
        var createResponse = await _client.Indices.CreateAsync(destination,
            d => d.Settings(s => s.NumberOfReplicas(0).NumberOfShards(1)), TestCancellationToken);
        Assert.True(createResponse.IsValidResponse, createResponse.GetErrorMessage());
        RegisterCompatibilityIndex(index);

        // Act: the versioned destination already exists, so the upgrade must fail before reindexing into it
        var exception = await Assert.ThrowsAsync<RepositoryException>(() => _configuration.UpgradeIndexCompatibilityAsync([index], cancellationToken: TestCancellationToken));

        // Assert
        Assert.Contains(destination, exception.Message);
        var sourceResponse = await _client.Indices.GetAsync((Indices)name, cancellationToken: TestCancellationToken);
        Assert.Contains(name, sourceResponse.Indices.Keys.Select(k => k.ToString()));
        var destinationResponse = await _client.Indices.GetAsync((Indices)destination, cancellationToken: TestCancellationToken);
        Assert.Contains(destination, destinationResponse.Indices.Keys.Select(k => k.ToString()));
    }

    [Fact]
    public async Task UpgradeIndexCompatibilityAsync_WhenSourceIsClosed_ThrowsBeforeChanges()
    {
        // Arrange
        string name = $"compat-closed-{Guid.NewGuid():N}";
        var index = new ForcedIncompatibleEmployeeIndex(_configuration, name);
        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());
        await index.ConfigureAsync();
        var compatibility = Assert.Single(await index.GetIndexCompatibilityAsync(TestCancellationToken));
        string targetIndex = CompatibilityIndexName.Create(name, compatibility.ServerMajor);
        var closeResponse = await _client.Indices.CloseAsync(name, cancellationToken: TestCancellationToken);
        Assert.True(closeResponse.IsValidResponse, closeResponse.GetErrorMessage());
        Assert.True(closeResponse.Acknowledged, closeResponse.DebugInformation);
        RegisterCompatibilityIndex(index);

        // Act: a closed index cannot be safely upgraded
        var exception = await Assert.ThrowsAsync<RepositoryException>(() =>
            _configuration.UpgradeIndexCompatibilityAsync([index], cancellationToken: TestCancellationToken));

        // Assert
        Assert.Contains("must be opened before using the Foundatio compatibility upgrader", exception.Message);
        await AssertIndexExistsAsync(targetIndex, false);
        var countResponse = await _client.CountAsync<object>(d => d.Indices(name), TestCancellationToken);
        Assert.False(countResponse.IsValidResponse, countResponse.DebugInformation);
        Assert.Equal("index_closed_exception", countResponse.ElasticsearchServerError?.Error?.Type);
    }

    [Fact]
    public async Task UpgradeIndexCompatibilityAsync_WhenCreateFromIsUnsupported_ThrowsBeforeWriteBlock()
    {
        // Arrange
        string name = $"compat-unsupported-{Guid.NewGuid():N}";
        var index = new UnsupportedCreateFromVersionEmployeeIndex(_configuration, name);
        await index.DeleteAsync();
        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());
        await index.ConfigureAsync();
        RegisterCompatibilityIndex(index);

        // Act: the target server version predates the create_from API, so the upgrade must fail before blocking writes
        var exception = await Assert.ThrowsAsync<NotSupportedException>(() =>
            _configuration.UpgradeIndexCompatibilityAsync([index], cancellationToken: TestCancellationToken));

        // Assert
        Assert.Contains("8.18", exception.Message);
        var settingsResponse = await _client.Indices.GetSettingsAsync((Indices)name, cancellationToken: TestCancellationToken);
        Assert.True(settingsResponse.IsValidResponse, settingsResponse.GetErrorMessage());
        Assert.False(settingsResponse.Settings.Values.Single().Settings?.Index?.Blocks?.Write is true);
        var allIndexesResponse = await _client.Indices.GetAsync(Indices.All,
            d => d.LimitToNamesAndAliases(), TestCancellationToken);
        Assert.DoesNotContain(CompatibilityIndexName.Create(name, 8), allIndexesResponse.Indices.Keys.Select(k => k.ToString()));
    }

    [Fact]
    public async Task UpgradeIndexCompatibilityAsync_WhenSourceWriteBlockIsRemovedDuringReindex_FailsBeforeCutover()
    {
        // Arrange
        string name = $"compat-source-unblocked-{Guid.NewGuid():N}";
        var index = new ForcedIncompatibleEmployeeIndex(_configuration, name);
        await index.DeleteAsync();
        await index.ConfigureAsync();
        var repository = new EmployeeRepository(index);
        await repository.AddAsync(EmployeeGenerator.Generate(), o => o.ImmediateConsistency());
        var compatibility = Assert.Single(await index.GetIndexCompatibilityAsync(TestCancellationToken));
        string targetIndex = CompatibilityIndexName.Create(name, compatibility.ServerMajor);
        await using AsyncDisposableAction _ = new(async () =>
            await _client.Indices.DeleteAsync(Indices.Parse($"{name},{targetIndex}"), d => d.IgnoreUnavailable(), TestCancellationToken));
        bool sourceUnblocked = false;
        RegisterCompatibilityIndex(index);

        // Act: an external actor removes the source write block mid-reindex, which must be detected before cutover
        var exception = await Assert.ThrowsAsync<RepositoryException>(() => _configuration.UpgradeIndexCompatibilityAsync(
            [index],
            async (progress, message) =>
            {
                if (progress is not 90 || sourceUnblocked || message?.Contains("Total:", StringComparison.Ordinal) is not true)
                    return;

                var unblockResponse = await _client.Indices.PutSettingsAsync(name,
                    d => d.Settings(s => s.Blocks(b => b.Write(false))), TestCancellationToken);
                Assert.True(unblockResponse.IsValidResponse, unblockResponse.GetErrorMessage());
                sourceUnblocked = true;
            },
            TestCancellationToken));

        // Assert
        Assert.True(sourceUnblocked);
        Assert.Contains("lost its write block", exception.ToString());
        await AssertIndexExistsAsync(name, true);
        await AssertIndexExistsAsync(targetIndex, true);
        var recoveryStatus = await _configuration.InspectIndexCompatibilityUpgradeAsync(index, name, TestCancellationToken);
        Assert.Equal(IndexCompatibilityRecoveryAction.ManualIntervention, recoveryStatus.Action);
    }

    [Fact]
    public async Task UpgradeIndexCompatibilityAsync_WhenSourceSettingsChangeBeforeCutover_FailsBeforeDeletingSource()
    {
        // Arrange
        string name = $"compat-source-settings-change-{Guid.NewGuid():N}";
        var index = new ForcedIncompatibleEmployeeIndex(_configuration, name);
        await index.DeleteAsync();
        await index.ConfigureAsync();
        var repository = new EmployeeRepository(index);
        await repository.AddAsync(EmployeeGenerator.Generate(), o => o.ImmediateConsistency());
        var compatibility = Assert.Single(await index.GetIndexCompatibilityAsync(TestCancellationToken));
        string targetIndex = CompatibilityIndexName.Create(name, compatibility.ServerMajor);
        await using AsyncDisposableAction _ = new(async () =>
            await _client.Indices.DeleteAsync(Indices.Parse($"{name},{targetIndex}"), d => d.IgnoreUnavailable(), TestCancellationToken));
        bool sourceSettingsChanged = false;
        RegisterCompatibilityIndex(index);

        // Act: an external actor changes the source's explicit settings mid-reindex, which must fail before deleting it
        var exception = await Assert.ThrowsAsync<RepositoryException>(() => _configuration.UpgradeIndexCompatibilityAsync(
            [index],
            async (progress, message) =>
            {
                if (progress is not 90 || sourceSettingsChanged || message?.Contains("Total:", StringComparison.Ordinal) is not true)
                    return;

                var updateResponse = await _client.Indices.PutSettingsAsync(name,
                    d => d.Settings(new IndexSettings
                    {
                        OtherSettings = new Dictionary<string, object> { ["index.max_result_window"] = 12345 }
                    }), TestCancellationToken);
                Assert.True(updateResponse.IsValidResponse, updateResponse.GetErrorMessage());
                sourceSettingsChanged = true;
            },
            TestCancellationToken));

        // Assert
        Assert.True(sourceSettingsChanged);
        Assert.Contains("explicit settings", exception.ToString(), StringComparison.OrdinalIgnoreCase);
        await AssertIndexExistsAsync(name, true);
        await AssertIndexExistsAsync(targetIndex, false);
    }

    [Fact]
    public async Task DeleteAsync_WithOrdinarySingleTargetAlias_DeletesBackingIndex()
    {
        // Arrange
        string name = $"compat-ordinary-alias-{Guid.NewGuid():N}";
        string physicalIndex = $"{name}-v1";
        using var index = new Index<object>(_configuration, name);
        var createResponse = await _client.Indices.CreateAsync(physicalIndex,
            d => d.Aliases(a => a.Add(name, new Alias())), TestCancellationToken);
        Assert.True(createResponse.IsValidResponse, createResponse.GetErrorMessage());
        await using AsyncDisposableAction _ = new(async () =>
            await _client.Indices.DeleteAsync(physicalIndex, d => d.IgnoreUnavailable(), TestCancellationToken));

        // Act
        await index.DeleteAsync();

        // Assert
        await AssertIndexExistsAsync(physicalIndex, false);
    }

    [Fact]
    public async Task CleanupIndexesJob_WithNaturallyPrefixedConfiguredName_UsesNativeName()
    {
        // Arrange
        const string prefix = "reindexed-v8-natural-logs";
        string indexName = $"{prefix}-2020.01.01";
        var createResponse = await _client.Indices.CreateAsync(indexName, cancellationToken: TestCancellationToken);
        Assert.True(createResponse.IsValidResponse, createResponse.GetErrorMessage());
        await using AsyncDisposableAction _ = new(async () =>
            await _client.Indices.DeleteAsync(indexName, d => d.IgnoreUnavailable(), TestCancellationToken));
        var job = new CompatibilityCleanupJob(_client, prefix);

        // Act
        await job.RunAsync(TestCancellationToken);

        // Assert
        Assert.Equal([indexName], job.DeletedIndexes);
        await AssertIndexExistsAsync(indexName, false);
    }
}
