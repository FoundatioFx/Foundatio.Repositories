using System;
using System.Linq;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Exceptions;
using Foundatio.Utility;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class IndexCompatibilityRecoveryTests : ElasticRepositoryTestBase
{
    public IndexCompatibilityRecoveryTests(ITestOutputHelper output) : base(output) { }

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        await RemoveDataAsync(false);
    }

    [Fact]
    public async Task RecoverIndexCompatibilityUpgradeAsync_WithOwnedInterruptedPreCutoverState_ResetsOnlyConfirmedArtifacts()
    {
        // Arrange
        string name = $"compat-recovery-{Guid.NewGuid():N}";
        using var index = new Index<object>(_configuration, name);
        await index.ConfigureAsync();
        var compatibility = Assert.Single(await index.GetIndexCompatibilityAsync(TestCancellationToken));
        string targetIndex = CompatibilityIndexName.Create(name, compatibility.ServerMajor);
        await using AsyncDisposableAction _ = new(async () =>
            await _client.Indices.DeleteAsync(Indices.Parse($"{name},{targetIndex}"), d => d.IgnoreUnavailable(), TestCancellationToken));

        var blockResponse = await _client.Indices.PutSettingsAsync(name,
            d => d.Settings(s => s.Blocks(b => b.Write(true))), TestCancellationToken);
        Assert.True(blockResponse.IsValidResponse);
        var createResponse = await _client.Indices.CreateAsync(targetIndex, cancellationToken: TestCancellationToken);
        Assert.True(createResponse.IsValidResponse);
        var ownershipResponse = await _client.Indices.UpdateAliasesAsync(a => a.Actions(actions => actions.Add(add => add
            .Index(targetIndex)
            .Alias(ElasticIndexCompatibilityUpgrader.OwnershipAlias)
            .IsHidden(true))), TestCancellationToken);
        Assert.True(ownershipResponse.IsValidResponse);

        // Act
        var before = await _configuration.InspectIndexCompatibilityUpgradeAsync(index, name, TestCancellationToken);
        var after = await _configuration.RecoverIndexCompatibilityUpgradeAsync(index, name, removeWriteBlock: true, TestCancellationToken);

        // Assert
        Assert.Equal(IndexCompatibilityUpgradeRecoveryState.Interrupted, before.State);
        Assert.True(before.CanRecover);
        Assert.Equal(IndexCompatibilityUpgradeRecoveryState.Ready, after.State);
        Assert.True(after.SourceExists);
        Assert.False(after.SourceWriteBlocked);
        Assert.False(after.TargetExists);
    }

    [Fact]
    public async Task RecoverIndexCompatibilityUpgradeAsync_WithUnownedUnaliasedDestination_FailsClosed()
    {
        string name = $"compat-recovery-unowned-{Guid.NewGuid():N}";
        using var index = new Index<object>(_configuration, name);
        await index.ConfigureAsync();
        var compatibility = Assert.Single(await index.GetIndexCompatibilityAsync(TestCancellationToken));
        string targetIndex = CompatibilityIndexName.Create(name, compatibility.ServerMajor);
        await using AsyncDisposableAction _ = new(async () =>
            await _client.Indices.DeleteAsync(Indices.Parse($"{name},{targetIndex}"), d => d.IgnoreUnavailable(), TestCancellationToken));

        var createResponse = await _client.Indices.CreateAsync(targetIndex, cancellationToken: TestCancellationToken);
        Assert.True(createResponse.IsValidResponse);

        var status = await _configuration.InspectIndexCompatibilityUpgradeAsync(index, name, TestCancellationToken);
        var exception = await Assert.ThrowsAsync<RepositoryException>(() =>
            _configuration.RecoverIndexCompatibilityUpgradeAsync(index, name, removeWriteBlock: true, TestCancellationToken));

        Assert.Equal(IndexCompatibilityUpgradeRecoveryState.Ambiguous, status.State);
        Assert.False(status.CanRecover);
        Assert.Contains("cannot be recovered automatically", exception.Message);
        Assert.True((await _client.Indices.ExistsAsync(targetIndex, cancellationToken: TestCancellationToken)).Exists);
    }

    [Fact]
    public async Task RecoverIndexCompatibilityUpgradeAsync_WithAliasedDestination_FailsClosed()
    {
        // Arrange
        string name = $"compat-recovery-ambiguous-{Guid.NewGuid():N}";
        using var index = new Index<object>(_configuration, name);
        await index.ConfigureAsync();
        var compatibility = Assert.Single(await index.GetIndexCompatibilityAsync(TestCancellationToken));
        string targetIndex = CompatibilityIndexName.Create(name, compatibility.ServerMajor);
        await using AsyncDisposableAction _ = new(async () =>
            await _client.Indices.DeleteAsync(Indices.Parse($"{name},{targetIndex}"), d => d.IgnoreUnavailable(), TestCancellationToken));

        var createResponse = await _client.Indices.CreateAsync(targetIndex,
            d => d.Aliases(a => a.Add($"{name}-unexpected", new Alias())), TestCancellationToken);
        Assert.True(createResponse.IsValidResponse);

        // Act
        var status = await _configuration.InspectIndexCompatibilityUpgradeAsync(index, name, TestCancellationToken);
        var exception = await Assert.ThrowsAsync<RepositoryException>(() =>
            _configuration.RecoverIndexCompatibilityUpgradeAsync(index, name, removeWriteBlock: true, TestCancellationToken));

        // Assert
        Assert.Equal(IndexCompatibilityUpgradeRecoveryState.Ambiguous, status.State);
        Assert.False(status.CanRecover);
        Assert.Contains("cannot be recovered automatically", exception.Message);
        Assert.True((await _client.Indices.ExistsAsync(targetIndex, cancellationToken: TestCancellationToken)).Exists);
    }

    [Fact]
    public async Task InspectIndexCompatibilityUpgradeAsync_WithOnlyUnexpectedAliasedTarget_IsAmbiguous()
    {
        string name = $"compat-recovery-unexpected-target-{Guid.NewGuid():N}";
        using var index = new Index<object>(_configuration, name);
        await index.ConfigureAsync();
        var compatibility = Assert.Single(await index.GetIndexCompatibilityAsync(TestCancellationToken));
        string targetIndex = CompatibilityIndexName.Create(name, compatibility.ServerMajor);
        await using AsyncDisposableAction _ = new(async () =>
            await _client.Indices.DeleteAsync(Indices.Parse($"{name},{targetIndex}"), d => d.IgnoreUnavailable(), TestCancellationToken));

        var deleteResponse = await _client.Indices.DeleteAsync(name, cancellationToken: TestCancellationToken);
        Assert.True(deleteResponse.IsValidResponse);
        var createResponse = await _client.Indices.CreateAsync(targetIndex,
            d => d.Aliases(a => a.Add($"unexpected-{name}", new Alias())), TestCancellationToken);
        Assert.True(createResponse.IsValidResponse);

        var status = await _configuration.InspectIndexCompatibilityUpgradeAsync(index, name, TestCancellationToken);

        Assert.Equal(IndexCompatibilityUpgradeRecoveryState.Ambiguous, status.State);
        Assert.False(status.CanRecover);
    }

    [Fact]
    public async Task RecoverIndexCompatibilityUpgradeAsync_WithCompletedTargetStillOwned_FailsClosed()
    {
        string name = $"compat-recovery-impossible-completed-{Guid.NewGuid():N}";
        using var index = new Index<object>(_configuration, name);
        await index.ConfigureAsync();
        var compatibility = Assert.Single(await index.GetIndexCompatibilityAsync(TestCancellationToken));
        string targetIndex = CompatibilityIndexName.Create(name, compatibility.ServerMajor);
        await using AsyncDisposableAction _ = new(async () =>
            await _client.Indices.DeleteAsync(Indices.Parse($"{name},{targetIndex}"), d => d.IgnoreUnavailable(), TestCancellationToken));

        var deleteResponse = await _client.Indices.DeleteAsync(name, cancellationToken: TestCancellationToken);
        Assert.True(deleteResponse.IsValidResponse);
        var createResponse = await _client.Indices.CreateAsync(targetIndex, cancellationToken: TestCancellationToken);
        Assert.True(createResponse.IsValidResponse);
        var aliasResponse = await _client.Indices.UpdateAliasesAsync(a => a.Actions(
            action => action.Add(add => add.Index(targetIndex).Alias(name)),
            action => action.Add(add => add.Index(targetIndex).Alias(ElasticIndexCompatibilityUpgrader.OwnershipAlias).IsHidden(true))),
            cancellationToken: TestCancellationToken);
        Assert.True(aliasResponse.IsValidResponse);
        var blockResponse = await _client.Indices.PutSettingsAsync(targetIndex,
            d => d.Settings(s => s.Blocks(b => b.Write(true))), TestCancellationToken);
        Assert.True(blockResponse.IsValidResponse);

        var status = await _configuration.InspectIndexCompatibilityUpgradeAsync(index, name, TestCancellationToken);
        var exception = await Assert.ThrowsAsync<RepositoryException>(() =>
            _configuration.RecoverIndexCompatibilityUpgradeAsync(index, name, removeWriteBlock: true, TestCancellationToken));

        Assert.Equal(IndexCompatibilityUpgradeRecoveryState.Ambiguous, status.State);
        Assert.True(status.TargetOwnershipConfirmed);
        Assert.True(status.TargetWriteBlocked);
        Assert.False(status.CanRecover);
        Assert.Contains("cannot be recovered automatically", exception.Message);
        var settingsResponse = await _client.Indices.GetSettingsAsync((Indices)targetIndex, cancellationToken: TestCancellationToken);
        Assert.True(settingsResponse.Settings.Values.Single().Settings?.Index?.Blocks?.Write is true);
    }
}
