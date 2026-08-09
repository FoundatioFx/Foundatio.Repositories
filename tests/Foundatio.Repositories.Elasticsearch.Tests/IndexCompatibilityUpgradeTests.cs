using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Clients.Elasticsearch.Mapping;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Indexes;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Extensions;
using Foundatio.Utility;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class IndexCompatibilityUpgradeTests : ElasticRepositoryTestBase
{
    public IndexCompatibilityUpgradeTests(ITestOutputHelper output) : base(output) { }

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        await RemoveDataAsync(false);
    }

    [Fact]
    public async Task CanUpgradeIndexCompatibilityForPlainIndexAsync()
    {
        var index = new ForcedIncompatibleEmployeeIndex(_configuration, "compat-upgrade-employees");
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
        Assert.True(sourceMappingResponse.IsValidResponse);
        var sourceProperties = sourceMappingResponse.Mappings.Values.Single().Mappings.Properties;
        Assert.NotNull(sourceProperties);
        var expectedPropertyNames = sourceProperties.Select(p => p.Key.ToString()).Order().ToArray();

        await _configuration.UpgradeIndexCompatibilityAsync(new[] { index }, cancellationToken: TestCancellationToken);

        // The original nominal name should now resolve through an alias to the replacement physical index.
        var getResponse = await _client.Indices.GetAsync((Indices)index.Name, cancellationToken: TestCancellationToken);
        Assert.True(getResponse.IsValidResponse);
        Assert.NotNull(getResponse.Indices);
        Assert.Single(getResponse.Indices);
        Assert.Equal(targetIndex, getResponse.Indices.Keys.Single().ToString());

        var countResponse = await _client.CountAsync<Employee>(d => d.Indices(index.Name), cancellationToken: TestCancellationToken);
        _logger.LogRequest(countResponse);
        Assert.True(countResponse.IsValidResponse);
        Assert.Equal(1, countResponse.Count);

        var result = await repository.GetByIdAsync(employee.Id);
        Assert.NotNull(result);
        Assert.Equal(employee.Id, result.Id);

        var mappingResponse = await _client.Indices.GetMappingAsync<Employee>(m => m.Indices(targetIndex), cancellationToken: TestCancellationToken);
        _logger.LogRequest(mappingResponse);
        Assert.True(mappingResponse.IsValidResponse);
        var targetProperties = mappingResponse.Mappings.Values.Single().Mappings.Properties;
        Assert.NotNull(targetProperties);
        Assert.Equal(expectedPropertyNames, targetProperties.Select(p => p.Key.ToString()).Order());

        var allIndexesResponse = await _client.Indices.GetAsync(Indices.All,
            d => d.LimitToNamesAndAliases(), TestCancellationToken);
        Assert.DoesNotContain(compatibility.Name, allIndexesResponse.Indices.Keys.Select(k => k.ToString()));
        Assert.DoesNotContain(await index.GetIndexCompatibilityAsync(TestCancellationToken), i => i.RequiresReindexBeforeNextMajorUpgrade);
    }

    [Fact]
    public async Task CanUpgradeIndexCompatibilityForVersionedIndexAsync()
    {
        var index = new ForcedIncompatibleVersionedEmployeeIndex(_configuration, "compat-upgrade-versioned-employees", 1);
        await index.DeleteAsync();
        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());
        await index.ConfigureAsync();

        var repository = new EmployeeRepository(index);
        var employee = await repository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());
        Assert.NotNull(employee?.Id);

        var compatibility = Assert.Single(await index.GetIndexCompatibilityAsync(TestCancellationToken));
        Assert.Equal(index.VersionedName, compatibility.Name);
        string targetIndex = CompatibilityIndexName.Create(index.VersionedName, compatibility.ServerMajor);

        await _configuration.UpgradeIndexCompatibilityAsync(new[] { index }, cancellationToken: TestCancellationToken);

        var logicalAliasResponse = await _client.Indices.GetAsync((Indices)index.Name, cancellationToken: TestCancellationToken);
        Assert.True(logicalAliasResponse.IsValidResponse);
        Assert.Equal(targetIndex, logicalAliasResponse.Indices.Keys.Single().ToString());

        var versionedAliasResponse = await _client.Indices.GetAsync((Indices)index.VersionedName,
            d => d.IgnoreUnavailable(), TestCancellationToken);
        Assert.Empty(versionedAliasResponse.Indices);

        await index.ConfigureAsync();
        logicalAliasResponse = await _client.Indices.GetAsync((Indices)index.Name, cancellationToken: TestCancellationToken);
        Assert.Equal(targetIndex, logicalAliasResponse.Indices.Keys.Single().ToString());
        var allIndexesResponse = await _client.Indices.GetAsync(Indices.All,
            d => d.LimitToNamesAndAliases(), TestCancellationToken);
        Assert.DoesNotContain(index.VersionedName, allIndexesResponse.Indices.Keys.Select(k => k.ToString()));
        Assert.NotNull(index.MappingResolver.GetMapping("name"));

        var countResponse = await _client.CountAsync<Employee>(d => d.Indices(index.Name), cancellationToken: TestCancellationToken);
        _logger.LogRequest(countResponse);
        Assert.True(countResponse.IsValidResponse);
        Assert.Equal(1, countResponse.Count);

        Assert.Equal(1, await index.GetCurrentVersionAsync());
    }

    [Fact]
    public async Task GetIndexCompatibilityAsync_DoesNotRequireUpgrade_ForCurrentlyCreatedIndex()
    {
        var realIndex = new EmployeeIndex(_configuration);
        await realIndex.DeleteAsync();
        await using AsyncDisposableAction _ = new(() => realIndex.DeleteAsync());
        await realIndex.ConfigureAsync();

        var compatInfos = await realIndex.GetIndexCompatibilityAsync(TestCancellationToken);
        Assert.Single(compatInfos);
        Assert.False(compatInfos.Single().RequiresReindexBeforeNextMajorUpgrade);
    }

    [Fact]
    public async Task GetIndexCompatibilityAsync_UsesOneCurrentServerAndSettingsRequest()
    {
        using var configuration = new RequestCountingElasticConfiguration();
        using var index = new RequestCountingIndex(configuration, $"compat-request-count-{Guid.NewGuid():N}");
        configuration.AddIndex(index);
        await configuration.Client.WaitForReadyAsync(TestCancellationToken);
        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());
        await index.ConfigureAsync();
        configuration.ResetRequestCounts();

        var stopwatch = Stopwatch.StartNew();
        await index.GetIndexCompatibilityAsync(TestCancellationToken);
        stopwatch.Stop();

        Assert.Equal(1, configuration.InfoRequestCount);
        Assert.Equal(1, configuration.CompatibilitySettingsRequestCount);
        _logger.LogInformation("Cold explicit compatibility detection completed in {ElapsedMilliseconds} ms", stopwatch.Elapsed.TotalMilliseconds);

        stopwatch.Restart();
        await index.GetIndexCompatibilityAsync(TestCancellationToken);
        stopwatch.Stop();

        Assert.Equal(2, configuration.InfoRequestCount);
        Assert.Equal(2, configuration.CompatibilitySettingsRequestCount);
        _logger.LogInformation("Warm explicit compatibility detection completed in {ElapsedMilliseconds} ms", stopwatch.Elapsed.TotalMilliseconds);
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
        Assert.True(createResponse.IsValidResponse);

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
        Assert.Equal(0, configuration.CompatibilitySettingsRequestCount);
    }

    [Fact]
    public async Task CanUpgradeIndexCompatibilityForDailyIndexWithoutLegacyPhysicalAliasesAsync()
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
        Assert.True(addRoutedAliasResponse.IsValidResponse);

        var compatibility = Assert.Single(await index.GetIndexCompatibilityAsync(TestCancellationToken));
        Assert.Equal(oldPhysicalIndex, compatibility.Name);
        string targetIndex = CompatibilityIndexName.Create(oldPhysicalIndex, compatibility.ServerMajor);

        await _configuration.UpgradeIndexCompatibilityAsync([index], cancellationToken: TestCancellationToken);

        var datedAliasResponse = await _client.Indices.GetAsync((Indices)datedAlias, cancellationToken: TestCancellationToken);
        Assert.True(datedAliasResponse.IsValidResponse);
        Assert.Equal(targetIndex, datedAliasResponse.Indices.Keys.Single().ToString());

        var umbrellaAliasResponse = await _client.Indices.GetAsync((Indices)index.Name, cancellationToken: TestCancellationToken);
        Assert.True(umbrellaAliasResponse.IsValidResponse);
        Assert.Equal(targetIndex, umbrellaAliasResponse.Indices.Keys.Single().ToString());

        var windowedAliasResponse = await _client.Indices.GetAsync((Indices)windowedAlias, cancellationToken: TestCancellationToken);
        Assert.True(windowedAliasResponse.IsValidResponse);
        Assert.Equal(targetIndex, windowedAliasResponse.Indices.Keys.Single().ToString());

        var aliases = datedAliasResponse.Indices.Values.Single().Aliases;
        Assert.NotNull(aliases);
        Assert.Contains(datedAlias, aliases.Keys.Select(k => k.ToString()));
        Assert.Contains(index.Name, aliases.Keys.Select(k => k.ToString()));
        Assert.Contains(windowedAlias, aliases.Keys.Select(k => k.ToString()));
        Assert.DoesNotContain(oldPhysicalIndex, aliases.Keys.Select(k => k.ToString()));

        var routedAliasResponse = await _client.Indices.GetAliasAsync((Indices)targetIndex,
            d => d.Name(routedAlias), TestCancellationToken);
        Assert.True(routedAliasResponse.IsValidResponse);
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
        Assert.Empty(oldPhysicalResponse.Indices);
        Assert.NotNull(await repository.GetByIdAsync(employee.Id));

        await index.DeleteAsync();
        var deletedTargetResponse = await _client.Indices.GetAsync(Indices.All,
            d => d.LimitToNamesAndAliases(), TestCancellationToken);
        Assert.DoesNotContain(targetIndex, deletedTargetResponse.Indices.Keys.Select(k => k.ToString()));
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
        await _configuration.UpgradeIndexCompatibilityAsync([version1], cancellationToken: TestCancellationToken);

        Assert.Equal(compatibilityTarget, await version1.GetCurrentPhysicalIndexAsync(version1.Version));

        await version2.ConfigureAsync();
        await version2.ReindexAsync();

        var aliasResponse = await _client.Indices.GetAsync((Indices)name, cancellationToken: TestCancellationToken);
        Assert.True(aliasResponse.IsValidResponse);
        Assert.Equal(version2.VersionedName, aliasResponse.Indices.Keys.Single().ToString());
        var countResponse = await _client.CountAsync<Employee>(d => d.Indices(name), TestCancellationToken);
        Assert.True(countResponse.IsValidResponse);
        Assert.Equal(1, countResponse.Count);
        var allIndexesResponse = await _client.Indices.GetAsync(Indices.All,
            d => d.LimitToNamesAndAliases(), TestCancellationToken);
        Assert.DoesNotContain(compatibilityTarget, allIndexesResponse.Indices.Keys.Select(k => k.ToString()));
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
        Assert.True(defaultPipelineResponse.IsValidResponse);
        var finalPipelineResponse = await _client.Ingest.PutPipelineAsync(finalPipeline,
            p => p.Processors(processors => processors.Script(script => script.Source("ctx.age += 1"))), TestCancellationToken);
        Assert.True(finalPipelineResponse.IsValidResponse);
        var settingsResponse = await _client.Indices.PutSettingsAsync(name,
            p => p.Settings(settings => settings.DefaultPipeline(defaultPipeline).FinalPipeline(finalPipeline)), TestCancellationToken);
        Assert.True(settingsResponse.IsValidResponse);

        await _configuration.UpgradeIndexCompatibilityAsync([index], cancellationToken: TestCancellationToken);

        var copiedEmployee = await repository.GetByIdAsync(employee.Id, o => o.Cache(false));
        Assert.NotNull(copiedEmployee);
        Assert.Equal(37, copiedEmployee.Age);

        var targetSettingsResponse = await _client.Indices.GetSettingsAsync((Indices)name, cancellationToken: TestCancellationToken);
        Assert.True(targetSettingsResponse.IsValidResponse);
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
        Assert.True(sourceSettingsResponse.IsValidResponse);
        Assert.False(sourceSettingsResponse.Settings.Values.Single().Settings?.Index?.Blocks?.Write is true);

        var addedAfterCleanup = await repository.AddAsync(EmployeeGenerator.Generate(), o => o.ImmediateConsistency());
        Assert.NotNull(addedAfterCleanup);
    }

    [Fact]
    public async Task UpgradeIndexCompatibilityAsync_PreservesExistingWriteBlockOnReplacement()
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
        Assert.True(blockResponse.IsValidResponse);

        await _configuration.UpgradeIndexCompatibilityAsync([index], cancellationToken: TestCancellationToken);

        var targetSettingsResponse = await _client.Indices.GetSettingsAsync((Indices)name, cancellationToken: TestCancellationToken);
        Assert.True(targetSettingsResponse.IsValidResponse);
        Assert.True(targetSettingsResponse.Settings.Values.Single().Settings?.Index?.Blocks?.Write is true);
        Assert.NotNull(await repository.GetByIdAsync(employee.Id));

        var blockedWrite = await _client.IndexAsync(EmployeeGenerator.Generate(), d => d.Index(name), TestCancellationToken);
        Assert.False(blockedWrite.IsValidResponse);
        Assert.Equal("cluster_block_exception", blockedWrite.ElasticsearchServerError?.Error?.Type);
    }

    [Fact]
    public async Task UpgradeIndexCompatibilityAsync_WithPendingDailySchemaUpgrade_ThrowsBeforeChanges()
    {
        string name = $"compat-schema-precedence-{Guid.NewGuid():N}";
        var currentIndex = new DailyIndex<Employee>(_configuration, name, 1);
        await using AsyncDisposableAction _ = new(() => currentIndex.DeleteAsync());
        var repository = new EmployeeRepository(currentIndex);
        await repository.AddAsync(EmployeeGenerator.Default, o => o.ImmediateConsistency());

        using var nextIndex = new ForcedIncompatibleDailyEmployeeIndex(_configuration, name, 2);

        var compatibility = Assert.Single(await nextIndex.GetIndexCompatibilityAsync(TestCancellationToken));
        var exception = await Assert.ThrowsAsync<RepositoryException>(() => _configuration.UpgradeIndexCompatibilityAsync([nextIndex], cancellationToken: TestCancellationToken));

        Assert.Contains("schema reindex", exception.Message);
        var sourceResponse = await _client.Indices.GetAsync((Indices)compatibility.Name, cancellationToken: TestCancellationToken);
        Assert.Contains(compatibility.Name, sourceResponse.Indices.Keys.Select(k => k.ToString()));
        var settings = sourceResponse.Indices.Values.Single().Settings?.Index;
        Assert.False(settings?.Blocks?.Write is true);
    }

    [Fact]
    public async Task UpgradeIndexCompatibilityAsync_WhenCompatibilityRemains_Throws()
    {
        var index = new AlwaysIncompatibleEmployeeIndex(_configuration, $"compat-remains-{Guid.NewGuid():N}");
        await index.DeleteAsync();
        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());
        await index.ConfigureAsync();

        var exception = await Assert.ThrowsAsync<RepositoryException>(() => _configuration.UpgradeIndexCompatibilityAsync([index], cancellationToken: TestCancellationToken));

        Assert.Contains("did not complete", exception.Message);
    }

    [Fact]
    public async Task UpgradeIndexCompatibilityAsync_WhenDestinationExists_ThrowsBeforeReindex()
    {
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
        Assert.True(createResponse.IsValidResponse);

        var exception = await Assert.ThrowsAsync<RepositoryException>(() => _configuration.UpgradeIndexCompatibilityAsync([index], cancellationToken: TestCancellationToken));

        Assert.Contains(destination, exception.Message);
        var sourceResponse = await _client.Indices.GetAsync((Indices)name, cancellationToken: TestCancellationToken);
        Assert.Contains(name, sourceResponse.Indices.Keys.Select(k => k.ToString()));
        var destinationResponse = await _client.Indices.GetAsync((Indices)destination, cancellationToken: TestCancellationToken);
        Assert.Contains(destination, destinationResponse.Indices.Keys.Select(k => k.ToString()));
    }

    [Fact]
    public async Task UpgradeIndexCompatibilityAsync_WhenCreateFromIsUnsupported_ThrowsBeforeWriteBlock()
    {
        string name = $"compat-unsupported-{Guid.NewGuid():N}";
        var index = new UnsupportedCreateFromVersionEmployeeIndex(_configuration, name);
        await index.DeleteAsync();
        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());
        await index.ConfigureAsync();

        var exception = await Assert.ThrowsAsync<NotSupportedException>(() =>
            _configuration.UpgradeIndexCompatibilityAsync([index], cancellationToken: TestCancellationToken));

        Assert.Contains("8.18", exception.Message);
        var settingsResponse = await _client.Indices.GetSettingsAsync((Indices)name, cancellationToken: TestCancellationToken);
        Assert.True(settingsResponse.IsValidResponse);
        Assert.False(settingsResponse.Settings.Values.Single().Settings?.Index?.Blocks?.Write is true);
        var allIndexesResponse = await _client.Indices.GetAsync(Indices.All,
            d => d.LimitToNamesAndAliases(), TestCancellationToken);
        Assert.DoesNotContain(CompatibilityIndexName.Create(name, 8), allIndexesResponse.Indices.Keys.Select(k => k.ToString()));
    }

    private class ForcedIncompatibleEmployeeIndex : Index<Employee>
    {
        public ForcedIncompatibleEmployeeIndex(IElasticConfiguration configuration, string name) : base(configuration, name) { }

        public override void ConfigureIndexMapping(TypeMappingDescriptor<Employee> map)
        {
            map.Properties(p => p.SetupDefaults());
        }

        public override async Task<IReadOnlyCollection<IndexCompatibilityInfo>> GetIndexCompatibilityAsync(CancellationToken cancellationToken = default)
        {
            var infos = await base.GetIndexCompatibilityAsync(cancellationToken).AnyContext();
            return ForceOriginalIndexesIncompatible(infos);
        }
    }

    private sealed class ForcedIncompatibleVersionedEmployeeIndex : VersionedIndex<Employee>
    {
        public ForcedIncompatibleVersionedEmployeeIndex(IElasticConfiguration configuration, string name, int version) : base(configuration, name, version) { }

        public override void ConfigureIndexMapping(TypeMappingDescriptor<Employee> map)
        {
            map.Properties(p => p.SetupDefaults());
        }

        public override async Task<IReadOnlyCollection<IndexCompatibilityInfo>> GetIndexCompatibilityAsync(CancellationToken cancellationToken = default)
        {
            var infos = await base.GetIndexCompatibilityAsync(cancellationToken).AnyContext();
            return ForceOriginalIndexesIncompatible(infos);
        }
    }

    private sealed class ForcedIncompatibleDailyEmployeeIndex : DailyIndex<Employee>
    {
        public ForcedIncompatibleDailyEmployeeIndex(IElasticConfiguration configuration, string name, int version) : base(configuration, name, version) { }

        public override void ConfigureIndex(CreateIndexRequestDescriptor idx)
        {
            base.ConfigureIndex(idx.Settings(s => s.NumberOfReplicas(0).NumberOfShards(1)));
        }

        public override void ConfigureIndexMapping(TypeMappingDescriptor<Employee> map)
        {
            map.Properties(p => p.SetupDefaults());
        }

        public override async Task<IReadOnlyCollection<IndexCompatibilityInfo>> GetIndexCompatibilityAsync(CancellationToken cancellationToken = default)
        {
            var infos = await base.GetIndexCompatibilityAsync(cancellationToken).AnyContext();
            return ForceOriginalIndexesIncompatible(infos);
        }
    }

    private sealed class AlwaysIncompatibleEmployeeIndex : Index<Employee>
    {
        public AlwaysIncompatibleEmployeeIndex(IElasticConfiguration configuration, string name) : base(configuration, name) { }

        public override void ConfigureIndexMapping(TypeMappingDescriptor<Employee> map)
        {
            map.Properties(p => p.SetupDefaults());
        }

        public override async Task<IReadOnlyCollection<IndexCompatibilityInfo>> GetIndexCompatibilityAsync(CancellationToken cancellationToken = default)
        {
            var infos = await base.GetIndexCompatibilityAsync(cancellationToken).AnyContext();
            return infos.Select(i => i with { RequiresReindexBeforeNextMajorUpgrade = true }).ToArray();
        }
    }

    private sealed class UnsupportedCreateFromVersionEmployeeIndex : ForcedIncompatibleEmployeeIndex
    {
        public UnsupportedCreateFromVersionEmployeeIndex(IElasticConfiguration configuration, string name) : base(configuration, name) { }

        public override async Task<IReadOnlyCollection<IndexCompatibilityInfo>> GetIndexCompatibilityAsync(CancellationToken cancellationToken = default)
        {
            var infos = await base.GetIndexCompatibilityAsync(cancellationToken).AnyContext();
            return infos.Select(i => i with { ServerMajor = 8, ServerVersion = "8.17.9" }).ToArray();
        }
    }

    private static IReadOnlyCollection<IndexCompatibilityInfo> ForceOriginalIndexesIncompatible(IReadOnlyCollection<IndexCompatibilityInfo> infos)
    {
        return infos.Select(i => i with
        {
            RequiresReindexBeforeNextMajorUpgrade = !String.Equals(i.Name, CompatibilityIndexName.Create(i.Name, i.ServerMajor), StringComparison.Ordinal)
        }).ToArray();
    }

    private sealed class RequestCountingElasticConfiguration : ElasticConfiguration
    {
        private int _infoRequestCount;
        private int _compatibilitySettingsRequestCount;

        public int InfoRequestCount => _infoRequestCount;
        public int CompatibilitySettingsRequestCount => _compatibilitySettingsRequestCount;

        protected override void ConfigureSettings(ElasticsearchClientSettings settings)
        {
            base.ConfigureSettings(settings);
            settings.OnRequestCompleted(call =>
            {
                var uri = call.Uri;
                if (uri is null)
                    return;

                if (uri.AbsolutePath is "/")
                    Interlocked.Increment(ref _infoRequestCount);

                if (uri.Query.Contains("features=settings", StringComparison.Ordinal))
                    Interlocked.Increment(ref _compatibilitySettingsRequestCount);
            });
        }

        public void ResetRequestCounts()
        {
            Interlocked.Exchange(ref _infoRequestCount, 0);
            Interlocked.Exchange(ref _compatibilitySettingsRequestCount, 0);
        }
    }

    private sealed class RequestCountingIndex : Index<object>
    {
        public RequestCountingIndex(IElasticConfiguration configuration, string name) : base(configuration, name) { }

        public override void ConfigureIndex(CreateIndexRequestDescriptor idx)
        {
            base.ConfigureIndex(idx.Settings(s => s.NumberOfReplicas(0).NumberOfShards(1)));
        }
    }
}
