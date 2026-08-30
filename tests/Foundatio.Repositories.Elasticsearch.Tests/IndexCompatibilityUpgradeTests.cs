using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Transport;
using Elastic.Transport.Products.Elasticsearch;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Exceptions;
using Foundatio.Utility;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed partial class IndexCompatibilityUpgradeTests : ElasticRepositoryTestBase
{
    public IndexCompatibilityUpgradeTests(ITestOutputHelper output) : base(output) { }

    private void RegisterCompatibilityIndex(IIndex index)
    {
        var field = typeof(ElasticConfiguration).GetField("_indexes", BindingFlags.Instance | BindingFlags.NonPublic);
        var indexes = Assert.IsType<List<IIndex>>(field?.GetValue(_configuration));
        if (!indexes.Any(candidate => ReferenceEquals(candidate, index)))
            indexes.Add(index);
    }

    private void ReplaceRegisteredCompatibilityIndex(IIndex index)
    {
        var field = typeof(ElasticConfiguration).GetField("_indexes", BindingFlags.Instance | BindingFlags.NonPublic);
        var indexes = Assert.IsType<List<IIndex>>(field?.GetValue(_configuration));
        indexes.RemoveAll(candidate => String.Equals(candidate.Name, index.Name, StringComparison.OrdinalIgnoreCase));
        indexes.Add(index);
    }

    [Fact]
    public async Task DeleteAsync_WhenCompatibilityPatternsAreMissing_DeletesExistingVersionedIndex()
    {
        string name = $"compat-delete-{Guid.NewGuid():N}";
        var version1 = new ForcedIncompatibleVersionedEmployeeIndex(_configuration, name, 1);
        var version2 = new ForcedIncompatibleVersionedEmployeeIndex(_configuration, name, 2);
        await using AsyncDisposableAction _ = new(() => version2.DeleteAsync());
        await version1.ConfigureAsync();
        await AssertIndexExistsAsync(version1.VersionedName, true);

        await version2.DeleteAsync();

        await AssertIndexExistsAsync(version1.VersionedName, false);
    }

    [Fact]
    public async Task DeleteAsync_WithVersionWildcard_UsesExistingPublicDeletionScope()
    {
        string name = $"compat-delete-unmarked-{Guid.NewGuid():N}";
        var index = new ForcedIncompatibleVersionedEmployeeIndex(_configuration, name, 1);
        string unmarkedIndex = $"{index.VersionedName}-error";
        await using AsyncDisposableAction _ = new(async () =>
            await _client.Indices.DeleteAsync(Indices.Parse($"{index.VersionedName},{unmarkedIndex}"), d => d.IgnoreUnavailable(), TestCancellationToken));
        await index.ConfigureAsync();
        var createResponse = await _client.Indices.CreateAsync(unmarkedIndex, cancellationToken: TestCancellationToken);
        Assert.True(createResponse.IsValidResponse, createResponse.GetErrorMessage());

        await index.DeleteAsync();

        await AssertIndexExistsAsync(index.VersionedName, false);
        await AssertIndexExistsAsync(unmarkedIndex, false);
    }

    [Fact]
    public async Task GetIndexCompatibilityAsync_IncludesGeneratedErrorPartition_AndUpgradesIt()
    {
        // Arrange
        string name = $"compat-error-partition-{Guid.NewGuid():N}";
        var index = new ForcedIncompatibleVersionedEmployeeIndex(_configuration, name, 1);
        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());
        await index.ConfigureAsync();
        string errorPartition = $"{index.VersionedName}-error";
        var createResponse = await _client.Indices.CreateAsync(errorPartition,
            d => d.Aliases(a => a.Add(ElasticReindexer.ErrorIndexOwnershipAlias, new Alias { IsHidden = true })), TestCancellationToken);
        Assert.True(createResponse.IsValidResponse, createResponse.GetErrorMessage());

        // Act
        var compatibility = await index.GetIndexCompatibilityAsync(TestCancellationToken);
        var errorInfo = Assert.Single(compatibility, i => String.Equals(i.Name, errorPartition, StringComparison.Ordinal));
        Assert.True(errorInfo.RequiresReindexBeforeNextMajorUpgrade);
        string errorTarget = CompatibilityIndexName.Create(errorPartition, errorInfo.ServerMajor);

        RegisterCompatibilityIndex(index);
        await _configuration.UpgradeIndexCompatibilityAsync([index], cancellationToken: TestCancellationToken);

        // Assert
        await AssertIndexExistsAsync(errorTarget, true);
        var aliasResponse = await _client.Indices.GetAsync((Indices)errorPartition, cancellationToken: TestCancellationToken);
        Assert.Equal(errorTarget, aliasResponse.Indices.Keys.Single().ToString());
        Assert.DoesNotContain(await index.GetIndexCompatibilityAsync(TestCancellationToken), i => i.RequiresReindexBeforeNextMajorUpgrade);
    }

    [Fact]
    public async Task GetIndexCompatibilityAsync_WithUnmarkedErrorSuffixIndex_FailsClosed()
    {
        // Arrange
        string name = $"compat-unmarked-error-{Guid.NewGuid():N}";
        var index = new ForcedIncompatibleVersionedEmployeeIndex(_configuration, name, 1);
        string unmarkedIndex = $"{index.VersionedName}-error";
        await using AsyncDisposableAction _ = new(async () =>
        {
            await index.DeleteAsync();
            await _client.Indices.DeleteAsync(unmarkedIndex, d => d.IgnoreUnavailable(), TestCancellationToken);
        });
        await index.ConfigureAsync();
        var createResponse = await _client.Indices.CreateAsync(unmarkedIndex, cancellationToken: TestCancellationToken);
        Assert.True(createResponse.IsValidResponse, createResponse.GetErrorMessage());

        // Act
        var exception = await Assert.ThrowsAsync<RepositoryException>(() => index.GetIndexCompatibilityAsync(TestCancellationToken));

        // Assert
        Assert.Contains("ownership marker", exception.Message);
        await AssertIndexExistsAsync(unmarkedIndex, true);

        RegisterCompatibilityIndex(index);
        var recoveryStatus = await _configuration.InspectIndexCompatibilityUpgradeAsync(index, unmarkedIndex, TestCancellationToken);
        Assert.Equal(IndexCompatibilityRecoveryAction.ManualIntervention, recoveryStatus.Action);
        Assert.False(recoveryStatus.CanRecover);
    }

    [Fact]
    public async Task UpgradeIndexCompatibilityAsync_DoesNotTouchPhysicalIndexesOwnedBySiblingConfiguration()
    {
        // Arrange
        string name = $"compat-sibling-{Guid.NewGuid():N}";
        using var configuration = new EndpointAwareElasticConfiguration();
        var events = new ForcedIncompatibleVersionedEmployeeIndex(configuration, name, 1);
        var natural = new ForcedIncompatibleVersionedEmployeeIndex(configuration, $"reindexed-v7-{name}", 1);
        await using AsyncDisposableAction cleanup = new(async () =>
        {
            await events.DeleteAsync();
            await natural.DeleteAsync();
        });
        configuration.AddIndex(events);
        configuration.AddIndex(natural);
        await events.ConfigureAsync();
        await natural.ConfigureAsync();
        await AssertIndexExistsAsync(natural.VersionedName, true);

        var preflight = await events.GetIndexCompatibilityAsync(TestCancellationToken);

        // Act & Assert
        var info = Assert.Single(preflight);
        Assert.Equal(events.VersionedName, info.Name);
        string targetIndex = CompatibilityIndexName.Create(events.VersionedName, info.ServerMajor);

        await configuration.UpgradeIndexCompatibilityAsync([events], cancellationToken: TestCancellationToken);

        await AssertIndexExistsAsync(natural.VersionedName, true);
        var upgradedAlias = await _client.Indices.GetAsync((Indices)events.VersionedName, cancellationToken: TestCancellationToken);
        Assert.Equal(targetIndex, upgradedAlias.Indices.Keys.Single().ToString());
        Assert.DoesNotContain(await events.GetIndexCompatibilityAsync(TestCancellationToken), i => i.RequiresReindexBeforeNextMajorUpgrade);
    }

    [Fact]
    public async Task DeleteAsync_DoesNotDeletePhysicalIndexesOwnedBySiblingConfiguration()
    {
        // Arrange
        string name = $"compat-sibling-delete-{Guid.NewGuid():N}";
        using var configuration = new EndpointAwareElasticConfiguration();
        var events = new ForcedIncompatibleVersionedEmployeeIndex(configuration, name, 1);
        var natural = new ForcedIncompatibleVersionedEmployeeIndex(configuration, $"reindexed-v8-{name}", 1);
        await using AsyncDisposableAction _ = new(() => natural.DeleteAsync());
        configuration.AddIndex(events);
        configuration.AddIndex(natural);
        await events.ConfigureAsync();
        await natural.ConfigureAsync();

        // Act
        await events.DeleteAsync();

        // Assert
        await AssertIndexExistsAsync(events.VersionedName, false);
        await AssertIndexExistsAsync(natural.VersionedName, true);
    }

    [Fact]
    public async Task SchemaReindexAsync_DoesNotDiscoverPhysicalIndexesOwnedBySiblingConfiguration()
    {
        string name = $"schema-sibling-{Guid.NewGuid():N}";
        using var configuration = new EndpointAwareElasticConfiguration();
        var version1 = new ForcedIncompatibleVersionedEmployeeIndex(configuration, name, 1);
        var version2 = new ForcedIncompatibleVersionedEmployeeIndex(configuration, name, 2);
        var natural = new ForcedIncompatibleVersionedEmployeeIndex(configuration, $"reindexed-v7-{name}", 1);
        configuration.AddIndex(version2);
        configuration.AddIndex(natural);
        await using AsyncDisposableAction cleanup = new(async () =>
        {
            await version2.DeleteAsync();
            await natural.DeleteAsync();
        });
        await version1.ConfigureAsync();
        await natural.ConfigureAsync();
        await version2.ConfigureAsync();

        await version2.ReindexAsync();

        await AssertIndexExistsAsync(version2.VersionedName, true);
        await AssertIndexExistsAsync(natural.VersionedName, true);
    }

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        await RemoveDataAsync(false);
    }

    [Fact]
    public async Task CanUpgradeIndexCompatibilityForPlainIndexAsync()
    {
        var index = new ForcedIncompatibleEmployeeIndex(_configuration, $"compat-upgrade-employees-{Guid.NewGuid():N}");
        await index.DeleteAsync();
        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());
        await index.ConfigureAsync();

        var repository = new EmployeeRepository(index);
        var employee = await repository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());
        Assert.NotNull(employee?.Id);

        var compatibility = Assert.Single(await index.GetIndexCompatibilityAsync(TestCancellationToken));
        string targetIndex = CompatibilityIndexName.Create(index.Name, compatibility.ServerMajor);

        var sourceMappingResponse = await _client.Indices.GetMappingAsync<Employee>(m => m.Indices(index.Name), cancellationToken: TestCancellationToken);
        _logger.LogRequest(sourceMappingResponse);
        Assert.True(sourceMappingResponse.IsValidResponse, sourceMappingResponse.GetErrorMessage());
        var sourceProperties = sourceMappingResponse.Mappings.Values.Single().Mappings.Properties;
        Assert.NotNull(sourceProperties);
        var expectedPropertyNames = sourceProperties.Select(p => p.Key.ToString()).Order().ToArray();

        RegisterCompatibilityIndex(index);
        int finalProgress = 0;
        await _configuration.UpgradeIndexCompatibilityAsync(new[] { index }, (progress, _) =>
        {
            finalProgress = progress;
            return Task.CompletedTask;
        }, TestCancellationToken);
        Assert.Equal(100, finalProgress);

        // The original nominal name should now resolve through an alias to the replacement physical index.
        var getResponse = await _client.Indices.GetAsync((Indices)index.Name, cancellationToken: TestCancellationToken);
        Assert.True(getResponse.IsValidResponse, getResponse.GetErrorMessage());
        Assert.NotNull(getResponse.Indices);
        Assert.Single(getResponse.Indices);
        Assert.Equal(targetIndex, getResponse.Indices.Keys.Single().ToString());

        var countResponse = await _client.CountAsync<Employee>(d => d.Indices(index.Name), cancellationToken: TestCancellationToken);
        _logger.LogRequest(countResponse);
        Assert.True(countResponse.IsValidResponse, countResponse.GetErrorMessage());
        Assert.Equal(1, countResponse.Count);

        var result = await repository.GetByIdAsync(employee.Id);
        Assert.NotNull(result);
        Assert.Equal(employee.Id, result.Id);

        var mappingResponse = await _client.Indices.GetMappingAsync<Employee>(m => m.Indices(targetIndex), cancellationToken: TestCancellationToken);
        _logger.LogRequest(mappingResponse);
        Assert.True(mappingResponse.IsValidResponse, mappingResponse.GetErrorMessage());
        var targetProperties = mappingResponse.Mappings.Values.Single().Mappings.Properties;
        Assert.NotNull(targetProperties);
        Assert.Equal(expectedPropertyNames, targetProperties.Select(p => p.Key.ToString()).Order());

        var allIndexesResponse = await _client.Indices.GetAsync(Indices.All,
            d => d.LimitToNamesAndAliases(), TestCancellationToken);
        Assert.True(allIndexesResponse.IsValidResponse, allIndexesResponse.GetErrorMessage());
        Assert.DoesNotContain(compatibility.Name, allIndexesResponse.Indices.Keys.Select(k => k.ToString()));
        Assert.DoesNotContain(await index.GetIndexCompatibilityAsync(TestCancellationToken), i => i.RequiresReindexBeforeNextMajorUpgrade);

        var targetSettingsResponse = await _client.Indices.GetSettingsAsync((Indices)targetIndex,
            d => d.IncludeDefaults(false), TestCancellationToken);
        Assert.True(targetSettingsResponse.IsValidResponse, targetSettingsResponse.GetErrorMessage());
        var targetSettings = targetSettingsResponse.Settings.Values.Single().Settings?.Index;
        Assert.Null(targetSettings?.RefreshInterval);
        Assert.Null(targetSettings?.DefaultPipeline);
        Assert.Null(targetSettings?.FinalPipeline);

        var targetAliases = getResponse.Indices.Values.Single().Aliases;
        Assert.NotNull(targetAliases);
        Assert.DoesNotContain(ElasticIndexCompatibilityUpgrader.OwnershipAlias, targetAliases.Keys.Select(k => k.ToString()));
    }

    [Fact]
    public async Task CanUpgradeIndexCompatibilityForDynamicIndexAsync()
    {
        // Arrange
        string name = $"compat-upgrade-dynamic-{Guid.NewGuid():N}";
        var index = new ForcedIncompatibleDynamicEmployeeIndex(_configuration, name);
        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());
        var repository = new EmployeeRepository(index);
        var employee = await repository.AddAsync(EmployeeGenerator.Generate(), o => o.ImmediateConsistency());
        var compatibility = (await index.GetIndexCompatibilityAsync(TestCancellationToken)).Single();
        string targetIndex = CompatibilityIndexName.Create(name, compatibility.ServerMajor);

        // Act
        RegisterCompatibilityIndex(index);
        await _configuration.UpgradeIndexCompatibilityAsync([index], cancellationToken: TestCancellationToken);

        // Assert
        var aliasResponse = await _client.Indices.GetAsync((Indices)name, cancellationToken: TestCancellationToken);
        Assert.True(aliasResponse.IsValidResponse, aliasResponse.GetErrorMessage());
        Assert.Equal(targetIndex, aliasResponse.Indices.Keys.Single().ToString());
        Assert.NotNull(await repository.GetByIdAsync(employee.Id));
        Assert.DoesNotContain(await index.GetIndexCompatibilityAsync(TestCancellationToken), i => i.RequiresReindexBeforeNextMajorUpgrade);
    }

    [Fact]
    public async Task CanUpgradePlainIndexWhoseConfiguredNameLooksLikeCompatibilityPrefixAsync()
    {
        string name = $"reindexed-v8-compat-natural-{Guid.NewGuid():N}";
        var index = new ForcedIncompatibleEmployeeIndex(_configuration, name);
        await index.DeleteAsync();
        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());
        await index.ConfigureAsync();
        var compatibility = Assert.Single(await index.GetIndexCompatibilityAsync(TestCancellationToken));
        string targetIndex = CompatibilityIndexName.Create(name, compatibility.ServerMajor, name);

        RegisterCompatibilityIndex(index);
        await _configuration.UpgradeIndexCompatibilityAsync([index], cancellationToken: TestCancellationToken);

        Assert.Equal($"reindexed-v{compatibility.ServerMajor}-{name}", targetIndex);
        var aliasResponse = await _client.Indices.GetAsync((Indices)name, cancellationToken: TestCancellationToken);
        Assert.True(aliasResponse.IsValidResponse, aliasResponse.GetErrorMessage());
        Assert.Equal(targetIndex, aliasResponse.Indices.Keys.Single().ToString());
        var allIndexes = await _client.Indices.GetAsync(Indices.All, d => d.LimitToNamesAndAliases(), TestCancellationToken);
        Assert.Contains(targetIndex, allIndexes.Indices.Keys.Select(k => k.ToString()));
        Assert.DoesNotContain(name, allIndexes.Indices.Keys.Select(k => k.ToString()));
    }

    [Fact]
    public async Task CanUpgradeIndexCompatibilityForVersionedIndexAsync()
    {
        var index = new ForcedIncompatibleVersionedEmployeeIndex(_configuration, $"compat-upgrade-versioned-employees-{Guid.NewGuid():N}", 1);
        await index.DeleteAsync();
        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());
        await index.ConfigureAsync();

        var repository = new EmployeeRepository(index);
        var employee = await repository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());
        Assert.NotNull(employee?.Id);

        var compatibility = Assert.Single(await index.GetIndexCompatibilityAsync(TestCancellationToken));
        Assert.Equal(index.VersionedName, compatibility.Name);
        string targetIndex = CompatibilityIndexName.Create(index.VersionedName, compatibility.ServerMajor);

        RegisterCompatibilityIndex(index);
        await _configuration.UpgradeIndexCompatibilityAsync(new[] { index }, cancellationToken: TestCancellationToken);

        var logicalAliasResponse = await _client.Indices.GetAsync((Indices)index.Name, cancellationToken: TestCancellationToken);
        Assert.True(logicalAliasResponse.IsValidResponse, logicalAliasResponse.GetErrorMessage());
        Assert.Equal(targetIndex, logicalAliasResponse.Indices.Keys.Single().ToString());

        var versionedAliasResponse = await _client.Indices.GetAsync((Indices)index.VersionedName,
            d => d.IgnoreUnavailable(), TestCancellationToken);
        Assert.Equal(targetIndex, versionedAliasResponse.Indices.Keys.Single().ToString());

        await index.ConfigureAsync();
        logicalAliasResponse = await _client.Indices.GetAsync((Indices)index.Name, cancellationToken: TestCancellationToken);
        Assert.Equal(targetIndex, logicalAliasResponse.Indices.Keys.Single().ToString());
        var allIndexesResponse = await _client.Indices.GetAsync(Indices.All,
            d => d.LimitToNamesAndAliases(), TestCancellationToken);
        Assert.DoesNotContain(index.VersionedName, allIndexesResponse.Indices.Keys.Select(k => k.ToString()));
        Assert.NotNull(index.MappingResolver.GetMapping("name"));

        var countResponse = await _client.CountAsync<Employee>(d => d.Indices(index.Name), cancellationToken: TestCancellationToken);
        _logger.LogRequest(countResponse);
        Assert.True(countResponse.IsValidResponse, countResponse.GetErrorMessage());
        Assert.Equal(1, countResponse.Count);

        Assert.Equal(1, await index.GetCurrentVersionAsync());
    }

    [Fact]
    public async Task CanUpgradeRetainedInactiveVersionedIndexCompatibilityAsync()
    {
        // Arrange
        string name = $"compat-retained-version-{Guid.NewGuid():N}";
        var version1 = new ForcedIncompatibleVersionedEmployeeIndex(_configuration, name, 1);
        var version2 = new ForcedIncompatibleVersionedEmployeeIndex(_configuration, name, 2) { DiscardIndexesOnReindex = false };
        await using AsyncDisposableAction _ = new(() => version1.DeleteAsync());
        await using AsyncDisposableAction version2Scope = new(() => version2.DeleteAsync());
        await version1.ConfigureAsync();
        var repository = new EmployeeRepository(version1);
        var employee = await repository.AddAsync(EmployeeGenerator.Generate(), o => o.ImmediateConsistency());
        Assert.NotNull(employee?.Id);
        await version2.ConfigureAsync();
        await version2.ReindexAsync();
        await AssertIndexExistsAsync(version1.VersionedName, true);
        var compatibility = await version2.GetIndexCompatibilityAsync(TestCancellationToken);
        var version1Compatibility = Assert.Single(compatibility, i => String.Equals(i.Name, version1.VersionedName, StringComparison.Ordinal));
        var version2Compatibility = Assert.Single(compatibility, i => String.Equals(i.Name, version2.VersionedName, StringComparison.Ordinal));
        Assert.True(version1Compatibility.RequiresReindexBeforeNextMajorUpgrade);
        Assert.True(version2Compatibility.RequiresReindexBeforeNextMajorUpgrade);
        string version1Target = CompatibilityIndexName.Create(version1.VersionedName, version1Compatibility.ServerMajor, name);
        string version2Target = CompatibilityIndexName.Create(version2.VersionedName, version2Compatibility.ServerMajor, name);

        // Act
        RegisterCompatibilityIndex(version2);
        await _configuration.UpgradeIndexCompatibilityAsync([version2], cancellationToken: TestCancellationToken);

        // Assert
        var retainedAliasResponse = await _client.Indices.GetAsync((Indices)version1.VersionedName, cancellationToken: TestCancellationToken);
        Assert.Equal(version1Target, retainedAliasResponse.Indices.Keys.Single().ToString());
        var activeAliasResponse = await _client.Indices.GetAsync((Indices)name, cancellationToken: TestCancellationToken);
        Assert.Equal(version2Target, activeAliasResponse.Indices.Keys.Single().ToString());
        var countResponse = await _client.CountAsync<Employee>(d => d.Indices(version1.VersionedName), TestCancellationToken);
        Assert.True(countResponse.IsValidResponse, countResponse.GetErrorMessage());
        Assert.Equal(1, countResponse.Count);
        Assert.DoesNotContain(await version2.GetIndexCompatibilityAsync(TestCancellationToken), i => i.RequiresReindexBeforeNextMajorUpgrade);
    }

    [Fact]
    public async Task GetIndexCompatibilityAsync_DoesNotRequireUpgrade_ForCurrentlyCreatedIndex()
    {
        var realIndex = _configuration.Employees;
        await realIndex.DeleteAsync();
        await using AsyncDisposableAction _ = new(() => realIndex.DeleteAsync());
        await realIndex.ConfigureAsync();

        var compatInfos = await realIndex.GetIndexCompatibilityAsync(TestCancellationToken);
        Assert.Single(compatInfos);
        Assert.False(compatInfos.Single().RequiresReindexBeforeNextMajorUpgrade);
    }

    [Fact]
    public async Task GetIndexCompatibilityAsync_UsesFixedRequestBudget()
    {
        using var configuration = new RequestCountingElasticConfiguration();
        using var index = new RequestCountingIndex(configuration, $"compat-request-count-{Guid.NewGuid():N}");
        configuration.AddIndex(index);
        await configuration.Client.WaitForReadyAsync(TestCancellationToken);
        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());
        await index.ConfigureAsync();
        configuration.ResetRequestCounts();

        await index.GetIndexCompatibilityAsync(TestCancellationToken);

        Assert.Equal(1, configuration.InfoRequestCount);
        Assert.Equal(1, configuration.CompatibilityMetadataRequestCount);
        Assert.DoesNotContain(configuration.RequestPaths, path => path.Contains("reindexed-v", StringComparison.Ordinal));

        await index.GetIndexCompatibilityAsync(TestCancellationToken);

        Assert.Equal(2, configuration.InfoRequestCount);
        Assert.Equal(2, configuration.CompatibilityMetadataRequestCount);
    }

    [Fact]
    public async Task GetIndexCompatibilityAsync_WithMultipleDailyPartitions_StillUsesOneMetadataRequest()
    {
        using var configuration = new RequestCountingElasticConfiguration();
        string name = $"compat-daily-request-count-{Guid.NewGuid():N}";
        using var index = new DailyIndex<object>(configuration, name);
        string first = $"{name}-v1-2026.08.10";
        string second = $"{name}-v1-2026.08.11";
        await using AsyncDisposableAction _ = new(async () =>
            await configuration.Client.Indices.DeleteAsync(Indices.Parse($"{first},{second}"), d => d.IgnoreUnavailable(), TestCancellationToken));

        var firstResponse = await configuration.Client.Indices.CreateAsync(first, cancellationToken: TestCancellationToken);
        var secondResponse = await configuration.Client.Indices.CreateAsync(second, cancellationToken: TestCancellationToken);
        Assert.True(firstResponse.IsValidResponse, firstResponse.GetErrorMessage());
        Assert.True(secondResponse.IsValidResponse, secondResponse.GetErrorMessage());
        configuration.ResetRequestCounts();

        var compatibility = await index.GetIndexCompatibilityAsync(TestCancellationToken);

        Assert.Equal(2, compatibility.Count);
        Assert.Equal(1, configuration.InfoRequestCount);
        Assert.Equal(1, configuration.CompatibilityMetadataRequestCount);
    }

    [Fact]
    public async Task GetIndexCompatibilityAsync_IgnoresUnaliasedCompatibilityTarget()
    {
        using var configuration = new RequestCountingElasticConfiguration();
        string name = $"compat-orphan-{Guid.NewGuid():N}";
        using var index = new RequestCountingIndex(configuration, name);
        configuration.AddIndex(index);
        await configuration.Client.WaitForReadyAsync(TestCancellationToken);
        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());
        await index.ConfigureAsync();

        string orphanName = CompatibilityIndexName.Create(name, 99);
        await using AsyncDisposableAction orphanScope = new(async () =>
            await configuration.Client.Indices.DeleteAsync(orphanName, d => d.IgnoreUnavailable(), TestCancellationToken));
        var createResponse = await configuration.Client.Indices.CreateAsync(orphanName,
            d => d.Settings(s => s.NumberOfReplicas(0).NumberOfShards(1)), TestCancellationToken);
        Assert.True(createResponse.IsValidResponse, createResponse.GetErrorMessage());

        var compatibility = await index.GetIndexCompatibilityAsync(TestCancellationToken);

        var info = Assert.Single(compatibility);
        Assert.Equal(name, info.Name);
    }

    [Fact]
    public async Task ConfigureIndexesAsync_DoesNotIssueCompatibilityRequests()
    {
        using var configuration = new RequestCountingElasticConfiguration();
        using var index = new RequestCountingIndex(configuration, $"compat-configure-count-{Guid.NewGuid():N}");
        configuration.AddIndex(index);
        await configuration.Client.WaitForReadyAsync(TestCancellationToken);
        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());
        await index.ConfigureAsync();
        configuration.ResetRequestCounts();

        await configuration.ConfigureIndexesAsync([index]);

        Assert.Equal(0, configuration.InfoRequestCount);
        Assert.Equal(0, configuration.CompatibilityMetadataRequestCount);
        Assert.DoesNotContain(configuration.RequestPaths, path => path.Contains("reindexed-v", StringComparison.Ordinal));
    }

    [Fact]
    public async Task DeleteAsync_WithConcreteName_PerformsNoMetadataLookup()
    {
        using var configuration = new RequestCountingElasticConfiguration();
        using var index = new RequestCountingIndex(configuration, $"compat-delete-count-{Guid.NewGuid():N}");
        await configuration.Client.WaitForReadyAsync(TestCancellationToken);
        await index.ConfigureAsync();
        configuration.ResetRequestCounts();

        await index.DeleteAsync();

        string request = Assert.Single(configuration.RequestPaths);
        Assert.StartsWith("DELETE /", request, StringComparison.Ordinal);
        Assert.Equal(0, configuration.CompatibilityMetadataRequestCount);
    }

    [Fact]
    public async Task DeleteAsync_WithHiddenCanonicalDatedAlias_DeletesCompatibilityBackingIndex()
    {
        string name = $"compat-delete-hidden-{Guid.NewGuid():N}";
        string canonicalName = $"{name}-v1-2026.08.10";
        string physicalIndex = $"reindexed-v9-{canonicalName}";
        using var index = new DailyIndex<object>(_configuration, name, 1);
        await using AsyncDisposableAction _ = new(async () =>
            await _client.Indices.DeleteAsync(physicalIndex, d => d.IgnoreUnavailable(), TestCancellationToken));
        var createResponse = await _client.Indices.CreateAsync(physicalIndex,
            d => d
                .Settings(s => s.Hidden(true))
                .Aliases(a => a.Add(canonicalName, new Alias { IsHidden = true })), TestCancellationToken);
        Assert.True(createResponse.IsValidResponse, createResponse.GetErrorMessage());

        await index.DeleteAsync();

        var existsResponse = await _client.Indices.ExistsAsync(physicalIndex, cancellationToken: TestCancellationToken);
        Assert.False(existsResponse.Exists, existsResponse.DebugInformation);
    }

    [Fact]
    public async Task DeleteAsync_WhenCanonicalAliasHasMultipleTargets_FailsWithoutDeletingEitherTarget()
    {
        string name = $"compat-delete-multiple-{Guid.NewGuid():N}";
        string first = $"reindexed-v8-{name}";
        string second = $"reindexed-v9-{name}";
        using var index = new Index<object>(_configuration, name);
        await using AsyncDisposableAction _ = new(async () =>
            await _client.Indices.DeleteAsync(Indices.Parse($"{first},{second}"), d => d.IgnoreUnavailable(), TestCancellationToken));
        var firstCreate = await _client.Indices.CreateAsync(first, d => d.Aliases(a => a.Add(name, new Alias())), TestCancellationToken);
        var secondCreate = await _client.Indices.CreateAsync(second, d => d.Aliases(a => a.Add(name, new Alias())), TestCancellationToken);
        Assert.True(firstCreate.IsValidResponse, firstCreate.GetErrorMessage());
        Assert.True(secondCreate.IsValidResponse, secondCreate.GetErrorMessage());

        var exception = await Assert.ThrowsAsync<RepositoryException>(() => index.DeleteAsync());

        Assert.Contains("resolves to 2 concrete indexes", exception.Message);
        var firstExists = await _client.Indices.ExistsAsync(first, cancellationToken: TestCancellationToken);
        var secondExists = await _client.Indices.ExistsAsync(second, cancellationToken: TestCancellationToken);
        Assert.True(firstExists.Exists, firstExists.DebugInformation);
        Assert.True(secondExists.Exists, secondExists.DebugInformation);
    }

    [Fact]
    public async Task CleanupIndexesJob_DeletesCompatibilityIndexOnlyWithCanonicalDatedAlias()
    {
        string unmarked = "reindexed-v9-logs-2020.01.01";
        string marked = "reindexed-v9-logs-2020.01.02";
        await using AsyncDisposableAction _ = new(async () =>
            await _client.Indices.DeleteAsync(Indices.Parse($"{unmarked},{marked}"), d => d.IgnoreUnavailable(), TestCancellationToken));
        var unmarkedCreate = await _client.Indices.CreateAsync(unmarked, cancellationToken: TestCancellationToken);
        var markedCreate = await _client.Indices.CreateAsync(marked,
            d => d.Aliases(a => a.Add("logs-2020.01.02", new Alias())), TestCancellationToken);
        Assert.True(unmarkedCreate.IsValidResponse, unmarkedCreate.GetErrorMessage());
        Assert.True(markedCreate.IsValidResponse, markedCreate.GetErrorMessage());
        var job = new CompatibilityCleanupJob(_client);

        await job.RunAsync(TestCancellationToken);

        Assert.Equal([marked], job.DeletedIndexes);
        var unmarkedExists = await _client.Indices.ExistsAsync(unmarked, cancellationToken: TestCancellationToken);
        var markedExists = await _client.Indices.ExistsAsync(marked, cancellationToken: TestCancellationToken);
        Assert.True(unmarkedExists.Exists, unmarkedExists.DebugInformation);
        Assert.False(markedExists.Exists, markedExists.DebugInformation);
    }

    [Fact]
    public async Task CanUpgradeIndexCompatibilityForDailyIndexAndPreserveCanonicalPhysicalAliasAsync()
    {
        string name = $"compat-upgrade-daily-{Guid.NewGuid():N}";
        var index = new ForcedIncompatibleDailyEmployeeIndex(_configuration, name, 1);
        string windowedAlias = $"{name}-last7days";
        index.AddAlias(windowedAlias, TimeSpan.FromDays(7));
        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());
        var repository = new EmployeeRepository(index);
        var employee = await repository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());
        Assert.NotNull(employee?.Id);

        string datedAlias = index.GetIndex(employee);
        string oldPhysicalIndex = index.GetVersionedIndex(employee.CreatedUtc);
        string routedAlias = $"{name}-routed";
        var addRoutedAliasResponse = await _client.Indices.UpdateAliasesAsync(a => a.Actions(actions => actions.Add(add => add
            .Index(oldPhysicalIndex)
            .Alias(routedAlias)
            .Filter(q => q.Term(t => t.Field("companyId").Value(employee.CompanyId)))
            .IndexRouting("write-route")
            .SearchRouting("search-route")
            .IsHidden(false)
            .IsWriteIndex(true))), TestCancellationToken);
        Assert.True(addRoutedAliasResponse.IsValidResponse, addRoutedAliasResponse.GetErrorMessage());

        var compatibility = Assert.Single(await index.GetIndexCompatibilityAsync(TestCancellationToken));
        Assert.Equal(oldPhysicalIndex, compatibility.Name);
        string targetIndex = CompatibilityIndexName.Create(oldPhysicalIndex, compatibility.ServerMajor);

        RegisterCompatibilityIndex(index);
        await _configuration.UpgradeIndexCompatibilityAsync([index], cancellationToken: TestCancellationToken);

        var datedAliasResponse = await _client.Indices.GetAsync((Indices)datedAlias, cancellationToken: TestCancellationToken);
        Assert.True(datedAliasResponse.IsValidResponse, datedAliasResponse.GetErrorMessage());
        Assert.Equal(targetIndex, datedAliasResponse.Indices.Keys.Single().ToString());

        var umbrellaAliasResponse = await _client.Indices.GetAsync((Indices)index.Name, cancellationToken: TestCancellationToken);
        Assert.True(umbrellaAliasResponse.IsValidResponse, umbrellaAliasResponse.GetErrorMessage());
        Assert.Equal(targetIndex, umbrellaAliasResponse.Indices.Keys.Single().ToString());

        var windowedAliasResponse = await _client.Indices.GetAsync((Indices)windowedAlias, cancellationToken: TestCancellationToken);
        Assert.True(windowedAliasResponse.IsValidResponse, windowedAliasResponse.GetErrorMessage());
        Assert.Equal(targetIndex, windowedAliasResponse.Indices.Keys.Single().ToString());

        var aliases = datedAliasResponse.Indices.Values.Single().Aliases;
        Assert.NotNull(aliases);
        Assert.Contains(datedAlias, aliases.Keys.Select(k => k.ToString()));
        Assert.Contains(index.Name, aliases.Keys.Select(k => k.ToString()));
        Assert.Contains(windowedAlias, aliases.Keys.Select(k => k.ToString()));
        Assert.Contains(oldPhysicalIndex, aliases.Keys.Select(k => k.ToString()));

        var routedAliasResponse = await _client.Indices.GetAliasAsync((Indices)targetIndex,
            d => d.Name(routedAlias), TestCancellationToken);
        Assert.True(routedAliasResponse.IsValidResponse, routedAliasResponse.GetErrorMessage());
#if ELASTICSEARCH9
        var routedAliases = routedAliasResponse.Aliases;
#else
        var routedAliases = routedAliasResponse.Values;
#endif
        Assert.NotNull(routedAliases);
        var routedAliasDefinitions = routedAliases[targetIndex].Aliases;
        Assert.NotNull(routedAliasDefinitions);
        var routedAliasDefinition = routedAliasDefinitions[routedAlias];
        Assert.NotNull(routedAliasDefinition.Filter);
        Assert.Equal("write-route", routedAliasDefinition.IndexRouting?.ToString());
        Assert.False(routedAliasDefinition.IsHidden);
        Assert.True(routedAliasDefinition.IsWriteIndex);
        Assert.Equal("search-route", routedAliasDefinition.SearchRouting?.ToString());

        var oldPhysicalResponse = await _client.Indices.GetAsync((Indices)oldPhysicalIndex,
            d => d.IgnoreUnavailable(), TestCancellationToken);
        Assert.Equal(targetIndex, oldPhysicalResponse.Indices.Keys.Single().ToString());
        Assert.NotNull(await repository.GetByIdAsync(employee.Id));

        await index.DeleteAsync();
        var deletedTargetResponse = await _client.Indices.GetAsync(Indices.All,
            d => d.LimitToNamesAndAliases(), TestCancellationToken);
        Assert.DoesNotContain(targetIndex, deletedTargetResponse.Indices.Keys.Select(k => k.ToString()));
    }

    [Fact]
    public async Task CanUpgradeIndexCompatibilityForMonthlyIndexAsync()
    {
        // Arrange
        string name = $"compat-upgrade-monthly-{Guid.NewGuid():N}";
        var index = new ForcedIncompatibleMonthlyEmployeeIndex(_configuration, name, 1);
        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());
        var repository = new EmployeeRepository(index);
        var employee = await repository.AddAsync(EmployeeGenerator.Generate(), o => o.ImmediateConsistency());
        string sourceIndex = index.GetVersionedIndex(employee.CreatedUtc);
        var compatibility = (await index.GetIndexCompatibilityAsync(TestCancellationToken)).Single(i => i.Name == sourceIndex);
        string targetIndex = CompatibilityIndexName.Create(sourceIndex, compatibility.ServerMajor);

        // Act
        RegisterCompatibilityIndex(index);
        await _configuration.UpgradeIndexCompatibilityAsync([index], cancellationToken: TestCancellationToken);

        // Assert
        var physicalAliasResponse = await _client.Indices.GetAsync((Indices)sourceIndex, cancellationToken: TestCancellationToken);
        Assert.True(physicalAliasResponse.IsValidResponse, physicalAliasResponse.GetErrorMessage());
        Assert.Equal(targetIndex, physicalAliasResponse.Indices.Keys.Single().ToString());
        var logicalAliasResponse = await _client.Indices.GetAsync((Indices)name, cancellationToken: TestCancellationToken);
        Assert.True(logicalAliasResponse.IsValidResponse, logicalAliasResponse.GetErrorMessage());
        Assert.Equal(targetIndex, logicalAliasResponse.Indices.Keys.Single().ToString());
        Assert.NotNull(await repository.GetByIdAsync(employee.Id));
    }

    [Fact]
    public async Task SchemaReindexAfterCompatibilityUpgrade_UsesPrefixedPhysicalSource()
    {
        string name = $"compat-then-schema-{Guid.NewGuid():N}";
        var version1 = new ForcedIncompatibleVersionedEmployeeIndex(_configuration, name, 1);
        var version2 = new ForcedIncompatibleVersionedEmployeeIndex(_configuration, name, 2);
        await version1.DeleteAsync();
        await version2.DeleteAsync();
        await using AsyncDisposableAction version1Scope = new(() => version1.DeleteAsync());
        await using AsyncDisposableAction version2Scope = new(() => version2.DeleteAsync());
        await version1.ConfigureAsync();

        var repository = new EmployeeRepository(version1);
        var employee = await repository.AddAsync(EmployeeGenerator.Generate(), o => o.ImmediateConsistency());
        Assert.NotNull(employee);
        var compatibility = Assert.Single(await version1.GetIndexCompatibilityAsync(TestCancellationToken));
        string compatibilityTarget = CompatibilityIndexName.Create(version1.VersionedName, compatibility.ServerMajor);
        RegisterCompatibilityIndex(version1);
        await _configuration.UpgradeIndexCompatibilityAsync([version1], cancellationToken: TestCancellationToken);

        var versionedAlias = await _client.Indices.GetAsync((Indices)version1.VersionedName, cancellationToken: TestCancellationToken);
        Assert.Equal(compatibilityTarget, versionedAlias.Indices.Keys.Single().ToString());

        ReplaceRegisteredCompatibilityIndex(version2);
        await version2.ConfigureAsync();
        await version2.ReindexAsync();

        var aliasResponse = await _client.Indices.GetAsync((Indices)name, cancellationToken: TestCancellationToken);
        Assert.True(aliasResponse.IsValidResponse, aliasResponse.GetErrorMessage());
        Assert.Equal(version2.VersionedName, aliasResponse.Indices.Keys.Single().ToString());
        var countResponse = await _client.CountAsync<Employee>(d => d.Indices(name), TestCancellationToken);
        Assert.True(countResponse.IsValidResponse, countResponse.GetErrorMessage());
        Assert.Equal(1, countResponse.Count);
        var allIndexesResponse = await _client.Indices.GetAsync(Indices.All,
            d => d.LimitToNamesAndAliases(), TestCancellationToken);
        Assert.DoesNotContain(compatibilityTarget, allIndexesResponse.Indices.Keys.Select(k => k.ToString()));
        var retiredVersionAliasResponse = await _client.Indices.GetAsync((Indices)version1.VersionedName,
            d => d.IgnoreUnavailable(), TestCancellationToken);
        Assert.Empty(retiredVersionAliasResponse.Indices);
    }

    [Fact]
    public async Task UpgradeIndexCompatibilityAsync_DoesNotRunIndexPipelinesDuringCopyAndRestoresThem()
    {
        string name = $"compat-pipelines-{Guid.NewGuid():N}";
        string defaultPipeline = $"{name}-default";
        string finalPipeline = $"{name}-final";
        var index = new ForcedIncompatibleEmployeeIndex(_configuration, name);
        await index.DeleteAsync();
        await using AsyncDisposableAction indexScope = new(() => index.DeleteAsync());
        await using AsyncDisposableAction pipelineScope = new(async () =>
        {
            await _client.Ingest.DeletePipelineAsync(defaultPipeline, cancellationToken: TestCancellationToken);
            await _client.Ingest.DeletePipelineAsync(finalPipeline, cancellationToken: TestCancellationToken);
        });

        await index.ConfigureAsync();
        var repository = new EmployeeRepository(index);
        var employee = EmployeeGenerator.Generate(age: 37);
        employee = await repository.AddAsync(employee, o => o.ImmediateConsistency());
        Assert.NotNull(employee);

        var defaultPipelineResponse = await _client.Ingest.PutPipelineAsync(defaultPipeline,
            p => p.Processors(processors => processors.Script(script => script.Source("ctx.age += 1"))), TestCancellationToken);
        Assert.True(defaultPipelineResponse.IsValidResponse, defaultPipelineResponse.GetErrorMessage());
        var finalPipelineResponse = await _client.Ingest.PutPipelineAsync(finalPipeline,
            p => p.Processors(processors => processors.Script(script => script.Source("ctx.age += 1"))), TestCancellationToken);
        Assert.True(finalPipelineResponse.IsValidResponse, finalPipelineResponse.GetErrorMessage());
        var settingsResponse = await _client.Indices.PutSettingsAsync(name,
            p => p.Settings(settings => settings.DefaultPipeline(defaultPipeline).FinalPipeline(finalPipeline)), TestCancellationToken);
        Assert.True(settingsResponse.IsValidResponse, settingsResponse.GetErrorMessage());

        RegisterCompatibilityIndex(index);
        await _configuration.UpgradeIndexCompatibilityAsync([index], cancellationToken: TestCancellationToken);

        var copiedEmployee = await repository.GetByIdAsync(employee.Id, o => o.Cache(false));
        Assert.NotNull(copiedEmployee);
        Assert.Equal(37, copiedEmployee.Age);

        var targetSettingsResponse = await _client.Indices.GetSettingsAsync((Indices)name, cancellationToken: TestCancellationToken);
        Assert.True(targetSettingsResponse.IsValidResponse, targetSettingsResponse.GetErrorMessage());
        var targetSettings = targetSettingsResponse.Settings.Values.Single().Settings?.Index;
        Assert.Equal(defaultPipeline, targetSettings?.DefaultPipeline);
        Assert.Equal(finalPipeline, targetSettings?.FinalPipeline);

        var postUpgradeEmployee = EmployeeGenerator.Generate(age: 40);
        postUpgradeEmployee = await repository.AddAsync(postUpgradeEmployee, o => o.ImmediateConsistency());
        var storedPostUpgradeEmployee = await repository.GetByIdAsync(postUpgradeEmployee!.Id, o => o.Cache(false));
        Assert.NotNull(storedPostUpgradeEmployee);
        Assert.Equal(42, storedPostUpgradeEmployee.Age);
    }

    [Fact]
    public async Task UpgradeIndexCompatibilityAsync_WhenDestinationTemplateAddsSetting_FailsBeforeCopy()
    {
        string name = $"compat-template-setting-{Guid.NewGuid():N}";
        string templateName = $"{name}-template";
        var index = new ForcedIncompatibleEmployeeIndex(_configuration, name);
        await index.DeleteAsync();
        await using AsyncDisposableAction indexScope = new(() => index.DeleteAsync());
        await index.ConfigureAsync();
        var compatibility = Assert.Single(await index.GetIndexCompatibilityAsync(TestCancellationToken));
        string targetIndex = CompatibilityIndexName.Create(name, compatibility.ServerMajor);

        string templateBody = $$"""
            {
              "index_patterns": ["{{targetIndex}}"],
              "template": {
                "settings": {
                  "index.codec": "best_compression"
                }
              }
            }
            """;
        var putTemplateResponse = await _client.Transport.RequestAsync<ElasticsearchStringResponse>(
            new EndpointPath(Elastic.Transport.HttpMethod.PUT, $"/_index_template/{Uri.EscapeDataString(templateName)}"),
            PostData.String(templateBody), null, null, TestCancellationToken);
        Assert.True(putTemplateResponse.IsValidResponse, putTemplateResponse.DebugInformation);
        await using AsyncDisposableAction templateScope = new(async () =>
            await _client.Transport.RequestAsync<ElasticsearchStringResponse>(
                new EndpointPath(Elastic.Transport.HttpMethod.DELETE, $"/_index_template/{Uri.EscapeDataString(templateName)}"),
                null, null, null, TestCancellationToken));

        RegisterCompatibilityIndex(index);
        var exception = await Assert.ThrowsAsync<RepositoryException>(() =>
            _configuration.UpgradeIndexCompatibilityAsync([index], cancellationToken: TestCancellationToken));

        Assert.Contains("did not preserve explicit settings", exception.Message);
        await AssertIndexExistsAsync(name, true);
        await AssertIndexExistsAsync(targetIndex, false);
        var sourceSettingsResponse = await _client.Indices.GetSettingsAsync((Indices)name, cancellationToken: TestCancellationToken);
        Assert.False(sourceSettingsResponse.Settings.Values.Single().Settings?.Index?.Blocks?.Write is true);
    }

    [Fact]
    public async Task UpgradeIndexCompatibilityAsync_WhenDestinationAliasesChangeBeforeCutover_FailsBeforeDeletingSource()
    {
        string name = $"compat-target-alias-change-{Guid.NewGuid():N}";
        string unexpectedAlias = $"{name}-unexpected";
        var index = new ForcedIncompatibleEmployeeIndex(_configuration, name);
        await index.DeleteAsync();
        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());
        await index.ConfigureAsync();
        var repository = new EmployeeRepository(index);
        await repository.AddAsync(EmployeeGenerator.Generate(), o => o.ImmediateConsistency());
        var compatibility = Assert.Single(await index.GetIndexCompatibilityAsync(TestCancellationToken));
        string targetIndex = CompatibilityIndexName.Create(name, compatibility.ServerMajor);
        bool aliasAdded = false;

        RegisterCompatibilityIndex(index);
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

        Assert.True(aliasAdded);
        Assert.Contains("unexpected aliases before cutover", exception.Message);
        await AssertIndexExistsAsync(name, true);
        await AssertIndexExistsAsync(targetIndex, false);
    }

    [Fact]
    public async Task UpgradeIndexCompatibilityAsync_WhenLaterDestinationExists_PrevalidatesWholeBatchBeforeBlockingFirstSource()
    {
        string firstName = $"compat-batch-first-{Guid.NewGuid():N}";
        string secondName = $"compat-batch-second-{Guid.NewGuid():N}";
        using var first = new ForcedIncompatibleEmployeeIndex(_configuration, firstName);
        using var second = new ForcedIncompatibleEmployeeIndex(_configuration, secondName);
        await first.ConfigureAsync();
        await second.ConfigureAsync();
        var firstCompatibility = Assert.Single(await first.GetIndexCompatibilityAsync(TestCancellationToken));
        var secondCompatibility = Assert.Single(await second.GetIndexCompatibilityAsync(TestCancellationToken));
        string firstTarget = CompatibilityIndexName.Create(firstName, firstCompatibility.ServerMajor);
        string secondTarget = CompatibilityIndexName.Create(secondName, secondCompatibility.ServerMajor);
        await using AsyncDisposableAction _ = new(async () =>
            await _client.Indices.DeleteAsync(Indices.Parse($"{firstName},{secondName},{firstTarget},{secondTarget}"), d => d.IgnoreUnavailable(), TestCancellationToken));
        var createTarget = await _client.Indices.CreateAsync(secondTarget, cancellationToken: TestCancellationToken);
        Assert.True(createTarget.IsValidResponse, createTarget.GetErrorMessage());
        RegisterCompatibilityIndex(first);
        RegisterCompatibilityIndex(second);

        var exception = await Assert.ThrowsAsync<RepositoryException>(() =>
            _configuration.UpgradeIndexCompatibilityAsync([first, second], cancellationToken: TestCancellationToken));

        Assert.Contains("already exists", exception.Message);
        var firstState = await _client.Indices.GetAsync((Indices)firstName,
            d => d.Features(Feature.Aliases, Feature.Settings), TestCancellationToken);
        Assert.True(firstState.IsValidResponse, firstState.GetErrorMessage());
        var state = firstState.Indices.Values.Single();
        Assert.False(state.Settings?.Index?.Blocks?.Write is true);
        Assert.NotNull(state.Aliases);
        Assert.False(state.Aliases.ContainsKey(ElasticIndexCompatibilityUpgrader.OwnershipAlias));
        var firstTargetExists = await _client.Indices.ExistsAsync(firstTarget, cancellationToken: TestCancellationToken);
        Assert.False(firstTargetExists.Exists, firstTargetExists.DebugInformation);
    }

    [Fact]
    public async Task UpgradeIndexCompatibilityAsync_WhenCanceledDuringReindex_RemovesTargetAndWriteBlock()
    {
        string name = $"compat-cancel-{Guid.NewGuid():N}";
        var index = new ForcedIncompatibleEmployeeIndex(_configuration, name);
        await index.DeleteAsync();
        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());
        await index.ConfigureAsync();

        var repository = new EmployeeRepository(index);
        await repository.AddAsync(EmployeeGenerator.Generate(), o => o.ImmediateConsistency());
        var compatibility = Assert.Single(await index.GetIndexCompatibilityAsync(TestCancellationToken));
        string targetIndex = CompatibilityIndexName.Create(name, compatibility.ServerMajor);
        bool observedWriteBlock = false;

        RegisterCompatibilityIndex(index);
        var exception = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => _configuration.UpgradeIndexCompatibilityAsync(
            [index],
            async (_, message) =>
            {
                if (message?.Contains("Total:", StringComparison.Ordinal) is not true)
                    return;

                var blockedWrite = await _client.IndexAsync(EmployeeGenerator.Generate(), d => d.Index(name), TestCancellationToken);
                observedWriteBlock = !blockedWrite.IsValidResponse && blockedWrite.ElasticsearchServerError?.Error?.Type is "cluster_block_exception";
                throw new OperationCanceledException("Test cancellation after the reindex task started.");
            },
            TestCancellationToken));

        Assert.Contains("Test cancellation", exception.Message);
        Assert.True(observedWriteBlock);
        var allIndexesResponse = await _client.Indices.GetAsync(Indices.All,
            d => d.LimitToNamesAndAliases(), TestCancellationToken);
        Assert.DoesNotContain(targetIndex, allIndexesResponse.Indices.Keys.Select(k => k.ToString()));
        var sourceSettingsResponse = await _client.Indices.GetSettingsAsync((Indices)name, cancellationToken: TestCancellationToken);
        Assert.True(sourceSettingsResponse.IsValidResponse, sourceSettingsResponse.GetErrorMessage());
        Assert.False(sourceSettingsResponse.Settings.Values.Single().Settings?.Index?.Blocks?.Write is true);

        var addedAfterCleanup = await repository.AddAsync(EmployeeGenerator.Generate(), o => o.ImmediateConsistency());
        Assert.NotNull(addedAfterCleanup);
    }

    [Fact]
    public async Task UpgradeIndexCompatibilityAsync_WithExistingWriteBlock_RejectsBeforeMutation()
    {
        string name = $"compat-existing-block-{Guid.NewGuid():N}";
        var index = new ForcedIncompatibleEmployeeIndex(_configuration, name);
        await index.DeleteAsync();
        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());
        await index.ConfigureAsync();

        var repository = new EmployeeRepository(index);
        var employee = await repository.AddAsync(EmployeeGenerator.Generate(), o => o.ImmediateConsistency());
        Assert.NotNull(employee);

        var blockResponse = await _client.Indices.PutSettingsAsync(name,
            d => d.Settings(s => s.Blocks(b => b.Write(true))), TestCancellationToken);
        Assert.True(blockResponse.IsValidResponse, blockResponse.GetErrorMessage());

        var compatibility = Assert.Single(await index.GetIndexCompatibilityAsync(TestCancellationToken));
        string targetIndex = CompatibilityIndexName.Create(name, compatibility.ServerMajor);
        RegisterCompatibilityIndex(index);
        var exception = await Assert.ThrowsAsync<RepositoryException>(() =>
            _configuration.UpgradeIndexCompatibilityAsync([index], cancellationToken: TestCancellationToken));

        Assert.Contains("already has an index write block", exception.Message);
        var sourceSettingsResponse = await _client.Indices.GetSettingsAsync((Indices)name, cancellationToken: TestCancellationToken);
        Assert.True(sourceSettingsResponse.IsValidResponse, sourceSettingsResponse.GetErrorMessage());
        Assert.True(sourceSettingsResponse.Settings.Values.Single().Settings?.Index?.Blocks?.Write is true);
        await AssertIndexExistsAsync(targetIndex, false);
        Assert.NotNull(await repository.GetByIdAsync(employee.Id));

        var recoveryStatus = await _configuration.InspectIndexCompatibilityUpgradeAsync(index, name, TestCancellationToken);
        Assert.Equal(IndexCompatibilityRecoveryAction.ManualIntervention, recoveryStatus.Action);
        Assert.False(recoveryStatus.CanRecover);

        var blockedWrite = await _client.IndexAsync(EmployeeGenerator.Generate(), d => d.Index(name), TestCancellationToken);
        Assert.False(blockedWrite.IsValidResponse, blockedWrite.DebugInformation);
        Assert.Equal("cluster_block_exception", blockedWrite.ElasticsearchServerError?.Error?.Type);
    }

}
