using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Utility;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed partial class IndexCompatibilityUpgradeTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(5)]
    [InlineData(10)]
    [InlineData(92)]
    [InlineData(100)]
    public async Task UpgradeIndexCompatibilityAsync_WhenProgressCallbackThrows_LogsWarningAndCompletesUpgrade(int failureProgress)
    {
        // Arrange
        string name = $"compat-callback-{Guid.NewGuid():N}";
        using var index = new ForcedIncompatibleEmployeeIndex(_configuration, name);
        RegisterCompatibilityIndex(index);
        await index.ConfigureAsync();
        using var repository = new EmployeeRepository(index);
        var employee = await repository.AddAsync(EmployeeGenerator.Generate(), o => o.ImmediateConsistency());
        var compatibility = Assert.Single(await index.GetIndexCompatibilityAsync(TestCancellationToken));
        string targetIndex = CompatibilityIndexName.Create(name, compatibility.ServerMajor);
        await using AsyncDisposableAction cleanup = new(async () =>
            await _client.Indices.DeleteAsync(Indices.Parse($"{name},{targetIndex}"), d => d.IgnoreUnavailable(), TestCancellationToken));
        var callbackException = new InvalidOperationException("Progress observer failed.");
        bool callbackFailed = false;

        // Act
        var exception = await Record.ExceptionAsync(() => _configuration.UpgradeIndexCompatibilityAsync(
            [index],
            (progress, _) =>
            {
                if (progress == failureProgress)
                {
                    callbackFailed = true;
                    throw callbackException;
                }

                return Task.CompletedTask;
            }, TestCancellationToken));

        // Assert
        Assert.True(callbackFailed);
        Assert.Null(exception);
        var state = await _client.Indices.GetAsync((Indices)name, cancellationToken: TestCancellationToken);
        Assert.True(state.IsValidResponse, state.GetErrorMessage());
        Assert.Equal(targetIndex, state.Indices.Keys.Single());
        var physical = state.Indices.Values.Single();
        Assert.False(physical.Settings?.Index?.Blocks?.Write is true);
        Assert.DoesNotContain(ElasticIndexCompatibilityUpgrader.OwnershipAlias, physical.Aliases!.Keys);
        var document = await _client.GetAsync<Employee>(employee.Id, d => d.Index(name), TestCancellationToken);
        Assert.True(document.Found, document.GetErrorMessage());
        Assert.Equal(employee.Name, document.Source?.Name);
    }

    [Fact]
    public async Task UpgradeIndexCompatibilityAsync_WhenProgressCallbackCancels_PreservesCommittedOutcome()
    {
        // Arrange
        string name = $"compat-callback-{Guid.NewGuid():N}";
        using var index = new ForcedIncompatibleEmployeeIndex(_configuration, name);
        RegisterCompatibilityIndex(index);
        await index.ConfigureAsync();
        using var repository = new EmployeeRepository(index);
        var employee = await repository.AddAsync(EmployeeGenerator.Generate(), o => o.ImmediateConsistency());
        var compatibility = Assert.Single(await index.GetIndexCompatibilityAsync(TestCancellationToken));
        string targetIndex = CompatibilityIndexName.Create(name, compatibility.ServerMajor);
        await using AsyncDisposableAction cleanup = new(async () =>
            await _client.Indices.DeleteAsync(Indices.Parse($"{name},{targetIndex}"), d => d.IgnoreUnavailable(), TestCancellationToken));
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestCancellationToken);
        bool callbackFailed = false;

        // Act
        var exception = await Record.ExceptionAsync(() => _configuration.UpgradeIndexCompatibilityAsync(
            [index],
            (progress, _) =>
            {
                if (progress == 100)
                {
                    callbackFailed = true;
                    cancellation.Cancel();
                }

                return Task.CompletedTask;
            }, cancellation.Token));

        // Assert
        Assert.True(callbackFailed);
        Assert.IsAssignableFrom<OperationCanceledException>(exception);
        var state = await _client.Indices.GetAsync((Indices)name, cancellationToken: TestCancellationToken);
        Assert.True(state.IsValidResponse, state.GetErrorMessage());
        Assert.Equal(targetIndex, state.Indices.Keys.Single());
        var physical = state.Indices.Values.Single();
        Assert.False(physical.Settings?.Index?.Blocks?.Write is true);
        Assert.DoesNotContain(ElasticIndexCompatibilityUpgrader.OwnershipAlias, physical.Aliases!.Keys);
        await AssertIndexExistsAsync(targetIndex, true);
        var document = await _client.GetAsync<Employee>(employee.Id, d => d.Index(name), TestCancellationToken);
        Assert.True(document.Found, document.GetErrorMessage());
        Assert.Equal(employee.Name, document.Source?.Name);
    }
}
