using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration;
using Foundatio.Repositories.Exceptions;
using Foundatio.Utility;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class IndexCompatibilityRecoveryTests : ElasticRepositoryTestBase
{
    public IndexCompatibilityRecoveryTests(ITestOutputHelper output) : base(output) { }

    private async Task AssertIndexExistsAsync(ElasticsearchClient client, string index, bool expected)
    {
        var response = await client.Indices.ExistsAsync(index, cancellationToken: TestCancellationToken);
        Assert.True(expected ? response.Exists : !response.Exists, response.DebugInformation);
    }

    [Fact]
    public async Task RecoverIndexCompatibilityUpgradeAsync_WithOnlySourceMarker_RequiresManualIntervention()
    {
        // Arrange
        string name = $"compat-recovery-marker-only-{Guid.NewGuid():N}";
        var (configuration, index) = CreateRegisteredIndex(name);
        using (configuration)
        using (index)
        {
            var client = configuration.Client;
            await index.ConfigureAsync();
            await using AsyncDisposableAction _ = new(async () =>
                await client.Indices.DeleteAsync(name, d => d.IgnoreUnavailable(), TestCancellationToken));
            var sourceMarker = await client.Indices.UpdateAliasesAsync(a => a.Actions(action => action.Add(add => add
                .Index(name).Alias(ElasticIndexCompatibilityUpgrader.OwnershipAlias).IsHidden(true))), TestCancellationToken);
            Assert.True(sourceMarker.IsValidResponse, sourceMarker.GetErrorMessage());

            // Act: inspect the marked-but-unblocked source before attempting recovery
            var before = await configuration.InspectIndexCompatibilityUpgradeAsync(index, name, TestCancellationToken);

            // Assert
            Assert.Equal(IndexCompatibilityRecoveryAction.ManualIntervention, before.Action);
            Assert.False(before.CanRecover);

            // Act: recovery must refuse to touch an unrecoverable index
            var exception = await Assert.ThrowsAsync<RepositoryException>(() =>
                configuration.RecoverIndexCompatibilityUpgradeAsync(index, name, TestCancellationToken));

            // Assert
            Assert.Contains("ManualIntervention", exception.Message);

            // Act: inspect again to confirm the failed attempt left the topology untouched
            var after = await configuration.InspectIndexCompatibilityUpgradeAsync(index, name, TestCancellationToken);

            // Assert
            Assert.Equal(IndexCompatibilityRecoveryAction.ManualIntervention, after.Action);
            Assert.True(after.SourceExists);
            Assert.False(after.SourceWriteBlocked);
            Assert.True(after.SourceWorkflowMarkerPresent);
            Assert.False(after.TargetExists);
        }
    }

    [Fact]
    public async Task RecoverIndexCompatibilityUpgradeAsync_WithMarkedInterruptedAttempt_PreservesArtifactsWithoutTerminationEvidence()
    {
        // Arrange
        string name = $"compat-recovery-{Guid.NewGuid():N}";
        var (configuration, index) = CreateRegisteredIndex(name);
        using (configuration)
        using (index)
        {
            var client = configuration.Client;
            await index.ConfigureAsync();
            int serverMajor = Assert.Single(await index.GetIndexCompatibilityAsync(TestCancellationToken)).ServerMajor;
            string targetIndex = CompatibilityIndexName.Create(name, serverMajor);
            await using AsyncDisposableAction _ = DeleteAsync(client, name, targetIndex);

            var sourceMarker = await client.Indices.UpdateAliasesAsync(a => a.Actions(action => action.Add(add => add
                .Index(name).Alias(ElasticIndexCompatibilityUpgrader.OwnershipAlias).IsHidden(true))), TestCancellationToken);
            Assert.True(sourceMarker.IsValidResponse, sourceMarker.GetErrorMessage());
            var block = await client.Indices.PutSettingsAsync(name,
                d => d.Settings(s => s.Blocks(b => b.Write(true))), TestCancellationToken);
            Assert.True(block.IsValidResponse, block.GetErrorMessage());
            var create = await client.Indices.CreateAsync(targetIndex, cancellationToken: TestCancellationToken);
            Assert.True(create.IsValidResponse, create.GetErrorMessage());
            var targetMarker = await client.Indices.UpdateAliasesAsync(a => a.Actions(action => action.Add(add => add
                .Index(targetIndex).Alias(ElasticIndexCompatibilityUpgrader.OwnershipAlias).IsHidden(true))), TestCancellationToken);
            Assert.True(targetMarker.IsValidResponse, targetMarker.GetErrorMessage());

            // Act: inspect before recovery, then attempt recovery of a source/target pair with no termination evidence
            var before = await configuration.InspectIndexCompatibilityUpgradeAsync(index, name, TestCancellationToken);
            await Assert.ThrowsAsync<RepositoryException>(() => configuration.RecoverIndexCompatibilityUpgradeAsync(index, name, TestCancellationToken));
            var after = await configuration.InspectIndexCompatibilityUpgradeAsync(index, name, TestCancellationToken);

            // Assert: both source and target artifacts survive untouched
            Assert.Equal(IndexCompatibilityRecoveryAction.ManualIntervention, before.Action);
            Assert.False(before.CanRecover);
            Assert.Equal(IndexCompatibilityRecoveryAction.ManualIntervention, after.Action);
            Assert.True(after.SourceExists);
            Assert.True(after.SourceWriteBlocked);
            Assert.True(after.SourceWorkflowMarkerPresent);
            Assert.True(after.TargetExists);
            Assert.True(after.TargetWorkflowMarkerPresent);
        }
    }

    [Fact]
    public async Task RecoverIndexCompatibilityUpgradeAsync_WithUnmarkedDestination_FailsClosed()
    {
        // Arrange
        string name = $"compat-recovery-unowned-{Guid.NewGuid():N}";
        var (configuration, index) = CreateRegisteredIndex(name);
        using (configuration)
        using (index)
        {
            var client = configuration.Client;
            await index.ConfigureAsync();
            int serverMajor = Assert.Single(await index.GetIndexCompatibilityAsync(TestCancellationToken)).ServerMajor;
            string targetIndex = CompatibilityIndexName.Create(name, serverMajor);
            await using AsyncDisposableAction _ = DeleteAsync(client, name, targetIndex);
            var create = await client.Indices.CreateAsync(targetIndex, cancellationToken: TestCancellationToken);
            Assert.True(create.IsValidResponse, create.GetErrorMessage());

            // Act: the destination exists but was never marked, so recovery must not assume ownership of it
            var status = await configuration.InspectIndexCompatibilityUpgradeAsync(index, name, TestCancellationToken);
            var exception = await Assert.ThrowsAsync<RepositoryException>(() =>
                configuration.RecoverIndexCompatibilityUpgradeAsync(index, name, TestCancellationToken));

            // Assert
            Assert.Equal(IndexCompatibilityRecoveryAction.ManualIntervention, status.Action);
            Assert.False(status.CanRecover);
            Assert.Contains("cannot be recovered automatically", exception.Message);
            await AssertIndexExistsAsync(client, targetIndex, true);
        }
    }

    [Fact]
    public async Task InspectIndexCompatibilityUpgradeAsync_WithUnexpectedResolvedSource_IsManualIntervention()
    {
        // Arrange
        string name = $"compat-recovery-source-alias-{Guid.NewGuid():N}";
        using var configuration = CreateConfiguration();
        using var index = new VersionedIndex<object>(configuration, name, 2);
        configuration.AddIndex(index);
        var client = configuration.Client;
        await index.ConfigureAsync();
        string sourceIndex = $"{name}-v1";
        await using AsyncDisposableAction _ = new(() => index.DeleteAsync());
        var aliasResponse = await client.Indices.UpdateAliasesAsync(a => a.Actions(action => action.Add(add => add
            .Index(index.VersionedName).Alias(sourceIndex))), TestCancellationToken);
        Assert.True(aliasResponse.IsValidResponse, aliasResponse.GetErrorMessage());

        // Act: the requested source name resolves to the current (already-upgraded) version through an alias
        var status = await configuration.InspectIndexCompatibilityUpgradeAsync(index, sourceIndex, TestCancellationToken);

        // Assert
        Assert.Equal(IndexCompatibilityRecoveryAction.ManualIntervention, status.Action);
        Assert.False(status.SourceExists);
        Assert.False(status.TargetExists);
        Assert.False(status.CanRecover);
    }

    [Fact]
    public async Task RecoverIndexCompatibilityUpgradeAsync_WithMarkedCompletedCutover_FinishesUnblockThenUnmarks()
    {
        // Arrange
        string name = $"compat-recovery-completed-{Guid.NewGuid():N}";
        var (configuration, index) = CreateRegisteredIndex(name);
        using (configuration)
        using (index)
        {
            var client = configuration.Client;
            await index.ConfigureAsync();
            int serverMajor = Assert.Single(await index.GetIndexCompatibilityAsync(TestCancellationToken)).ServerMajor;
            string targetIndex = CompatibilityIndexName.Create(name, serverMajor);
            await using AsyncDisposableAction _ = DeleteAsync(client, name, targetIndex);
            var delete = await client.Indices.DeleteAsync(name, cancellationToken: TestCancellationToken);
            Assert.True(delete.IsValidResponse, delete.GetErrorMessage());
            var create = await client.Indices.CreateAsync(targetIndex, cancellationToken: TestCancellationToken);
            Assert.True(create.IsValidResponse, create.GetErrorMessage());
            var aliases = await client.Indices.UpdateAliasesAsync(a => a.Actions(
                action => action.Add(add => add.Index(targetIndex).Alias(name)),
                action => action.Add(add => add.Index(targetIndex).Alias(ElasticIndexCompatibilityUpgrader.OwnershipAlias).IsHidden(true))),
                cancellationToken: TestCancellationToken);
            Assert.True(aliases.IsValidResponse, aliases.GetErrorMessage());
            var block = await client.Indices.PutSettingsAsync(targetIndex,
                d => d.Settings(s => s.Blocks(b => b.Write(true))), TestCancellationToken);
            Assert.True(block.IsValidResponse, block.GetErrorMessage());

            // Act: cutover already renamed the target to the canonical name, so recovery only needs to finish
            var before = await configuration.InspectIndexCompatibilityUpgradeAsync(index, name, TestCancellationToken);
            var after = await configuration.RecoverIndexCompatibilityUpgradeAsync(index, name, TestCancellationToken);

            // Assert
            Assert.Equal(IndexCompatibilityRecoveryAction.Finish, before.Action);
            Assert.True(before.TargetWorkflowMarkerPresent);
            Assert.True(before.TargetWriteBlocked);
            Assert.Equal(IndexCompatibilityRecoveryAction.None, after.Action);
            Assert.False(after.TargetWorkflowMarkerPresent);
            Assert.False(after.TargetWriteBlocked);
        }
    }

    [Fact]
    public async Task InspectIndexCompatibilityUpgradeAsync_WithErrorSource_AuthenticatesSurvivingSide()
    {
        // Arrange
        string name = $"compat-recovery-error-{Guid.NewGuid():N}";
        using var configuration = CreateConfiguration();
        using var index = new VersionedIndex<object>(configuration, name, 1);
        configuration.AddIndex(index);
        var client = configuration.Client;
        string sourceIndex = $"{index.VersionedName}-error";
        var create = await client.Indices.CreateAsync(sourceIndex, d => d.Aliases(a => a
            .Add(ElasticReindexer.ErrorIndexOwnershipAlias, new Alias { IsHidden = true })
            .Add(ElasticIndexCompatibilityUpgrader.OwnershipAlias, new Alias { IsHidden = true })), TestCancellationToken);
        Assert.True(create.IsValidResponse, create.GetErrorMessage());
        await using AsyncDisposableAction _ = new(async () =>
            await client.Indices.DeleteAsync(sourceIndex, d => d.IgnoreUnavailable(), TestCancellationToken));

        // Act: only the error-marked source survives, so inspection must authenticate it before recovery
        var status = await configuration.InspectIndexCompatibilityUpgradeAsync(index, sourceIndex, TestCancellationToken);

        // Assert
        Assert.Equal(IndexCompatibilityRecoveryAction.ManualIntervention, status.Action);
        Assert.True(status.SourceWorkflowMarkerPresent);
    }

    [Theory]
    [InlineData(true, IndexCompatibilityRecoveryAction.ManualIntervention)]
    [InlineData(false, IndexCompatibilityRecoveryAction.ManualIntervention)]
    public async Task InspectIndexCompatibilityUpgradeAsync_WithInterruptedErrorTarget_RequiresPersistentErrorMarker(
        bool includeTargetErrorMarker,
        IndexCompatibilityRecoveryAction expectedAction)
    {
        // Arrange
        string name = $"compat-recovery-error-partial-{Guid.NewGuid():N}";
        using var configuration = CreateConfiguration();
        using var index = new VersionedIndex<object>(configuration, name, 1);
        configuration.AddIndex(index);
        var client = configuration.Client;
        string sourceIndex = $"{index.VersionedName}-error";
        var source = await client.Indices.CreateAsync(sourceIndex, d => d
            .Settings(s => s.Blocks(b => b.Write(true)))
            .Aliases(a => a
                .Add(ElasticReindexer.ErrorIndexOwnershipAlias, new Alias { IsHidden = true })
                .Add(ElasticIndexCompatibilityUpgrader.OwnershipAlias, new Alias { IsHidden = true })), TestCancellationToken);
        Assert.True(source.IsValidResponse, source.GetErrorMessage());
        var infoResponse = await client.InfoAsync(TestCancellationToken);
        Assert.True(infoResponse.IsValidResponse, infoResponse.GetErrorMessage());
        int serverMajor = Foundatio.Repositories.Elasticsearch.Configuration.Index.ParseCreatedMajor(null, infoResponse.Version?.Number)!.Value;
        string targetIndex = CompatibilityIndexName.Create(sourceIndex, serverMajor, index.Name);
        var targetAliases = new Dictionary<string, Alias>
        {
            [ElasticIndexCompatibilityUpgrader.OwnershipAlias] = new() { IsHidden = true }
        };
        if (includeTargetErrorMarker)
            targetAliases[ElasticReindexer.ErrorIndexOwnershipAlias] = new() { IsHidden = true };
        var target = await client.Indices.CreateAsync(targetIndex, d => d.Aliases(a =>
        {
            foreach (var alias in targetAliases)
                a.Add(alias.Key, alias.Value);
        }), TestCancellationToken);
        Assert.True(target.IsValidResponse, target.GetErrorMessage());
        await using AsyncDisposableAction _ = DeleteAsync(client, sourceIndex, targetIndex);

        // Act: the target survived the interruption but its error marker depends on the theory case
        var status = await configuration.InspectIndexCompatibilityUpgradeAsync(index, sourceIndex, TestCancellationToken);

        // Assert
        Assert.Equal(expectedAction, status.Action);
    }

    [Theory]
    [InlineData(true, IndexCompatibilityRecoveryAction.Finish)]
    [InlineData(false, IndexCompatibilityRecoveryAction.ManualIntervention)]
    public async Task InspectIndexCompatibilityUpgradeAsync_WithSurvivingErrorTarget_RequiresPersistentErrorMarker(
        bool includeErrorMarker,
        IndexCompatibilityRecoveryAction expectedAction)
    {
        // Arrange
        string name = $"compat-recovery-error-target-{Guid.NewGuid():N}";
        using var configuration = CreateConfiguration();
        using var index = new VersionedIndex<object>(configuration, name, 1);
        configuration.AddIndex(index);
        var client = configuration.Client;
        string sourceIndex = $"{index.VersionedName}-error";
        var infoResponse = await client.InfoAsync(TestCancellationToken);
        Assert.True(infoResponse.IsValidResponse, infoResponse.GetErrorMessage());
        int serverMajor = Foundatio.Repositories.Elasticsearch.Configuration.Index.ParseCreatedMajor(null, infoResponse.Version?.Number)!.Value;
        string targetIndex = CompatibilityIndexName.Create(sourceIndex, serverMajor, index.Name);
        var aliases = new Dictionary<string, Alias>
        {
            [sourceIndex] = new(),
            [ElasticIndexCompatibilityUpgrader.OwnershipAlias] = new() { IsHidden = true }
        };
        if (includeErrorMarker)
            aliases[ElasticReindexer.ErrorIndexOwnershipAlias] = new() { IsHidden = true };
        var create = await client.Indices.CreateAsync(targetIndex, d => d.Aliases(a =>
        {
            foreach (var alias in aliases)
                a.Add(alias.Key, alias.Value);
        }), TestCancellationToken);
        Assert.True(create.IsValidResponse, create.GetErrorMessage());
        await using AsyncDisposableAction _ = new(async () =>
            await client.Indices.DeleteAsync(targetIndex, d => d.IgnoreUnavailable(), TestCancellationToken));

        // Act: the source is already gone, so only the target's error marker determines the outcome
        var status = await configuration.InspectIndexCompatibilityUpgradeAsync(index, sourceIndex, TestCancellationToken);

        // Assert
        Assert.Equal(expectedAction, status.Action);
    }

    [Fact]
    public async Task InspectIndexCompatibilityUpgradeAsync_WithMarkedDestinationFromDifferentMajor_IsManualIntervention()
    {
        // Arrange
        string name = $"compat-recovery-prior-major-{Guid.NewGuid():N}";
        var (configuration, index) = CreateRegisteredIndex(name);
        using (configuration)
        using (index)
        {
            var client = configuration.Client;
            await index.ConfigureAsync();
            var infoResponse = await client.InfoAsync(TestCancellationToken);
            Assert.True(infoResponse.IsValidResponse, infoResponse.GetErrorMessage());
            int serverMajor = Foundatio.Repositories.Elasticsearch.Configuration.Index.ParseCreatedMajor(null, infoResponse.Version?.Number)!.Value;
            string priorMajorTarget = CompatibilityIndexName.Create(name, serverMajor - 1);
            await using AsyncDisposableAction _ = DeleteAsync(client, name, priorMajorTarget);
            var sourceMarker = await client.Indices.UpdateAliasesAsync(a => a.Actions(action => action.Add(add => add
                .Index(name).Alias(ElasticIndexCompatibilityUpgrader.OwnershipAlias).IsHidden(true))), TestCancellationToken);
            Assert.True(sourceMarker.IsValidResponse, sourceMarker.GetErrorMessage());
            var sourceBlock = await client.Indices.PutSettingsAsync(name,
                d => d.Settings(s => s.Blocks(b => b.Write(true))), TestCancellationToken);
            Assert.True(sourceBlock.IsValidResponse, sourceBlock.GetErrorMessage());
            var target = await client.Indices.CreateAsync(priorMajorTarget,
                d => d.Aliases(a => a.Add(ElasticIndexCompatibilityUpgrader.OwnershipAlias, new Alias { IsHidden = true })), TestCancellationToken);
            Assert.True(target.IsValidResponse, target.GetErrorMessage());

            // Act: the marked destination targets a stale major, which cannot be the current upgrade's target
            var status = await configuration.InspectIndexCompatibilityUpgradeAsync(index, name, TestCancellationToken);

            // Assert
            Assert.Equal(IndexCompatibilityRecoveryAction.ManualIntervention, status.Action);
            Assert.Equal([priorMajorTarget], status.UnexpectedResolvedIndexes);
        }
    }

    private (MyAppElasticConfiguration Configuration, Index<object> Index) CreateRegisteredIndex(string name)
    {
        var configuration = CreateConfiguration();
        var index = new Index<object>(configuration, name);
        configuration.AddIndex(index);
        return (configuration, index);
    }

    private MyAppElasticConfiguration CreateConfiguration()
    {
        return new MyAppElasticConfiguration(_workItemQueue, _cache, _messageBus, Log);
    }

    private AsyncDisposableAction DeleteAsync(ElasticsearchClient client, string sourceIndex, string targetIndex)
    {
        return new AsyncDisposableAction(async () =>
            await client.Indices.DeleteAsync(Indices.Parse($"{sourceIndex},{targetIndex}"), d => d.IgnoreUnavailable(), TestCancellationToken));
    }
}
