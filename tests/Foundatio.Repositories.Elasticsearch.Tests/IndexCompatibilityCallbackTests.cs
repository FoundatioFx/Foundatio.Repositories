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
    [InlineData(0, false)]
    [InlineData(10, false)]
    [InlineData(92, false)]
    [InlineData(100, false)]
    [InlineData(100, true)]
    public async Task UpgradeIndexCompatibilityAsync_WhenProgressCallbackFailsOrCancels_PreservesCommittedOutcome(int failureProgress, bool cancel)
    {
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
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestCancellationToken);
        bool callbackFailed = false;

        var exception = await Record.ExceptionAsync(() => _configuration.UpgradeIndexCompatibilityAsync(
            [index],
            (progress, _) =>
            {
                if (progress == failureProgress)
                {
                    callbackFailed = true;
                    if (cancel)
                    {
                        cancellation.Cancel();
                        return Task.CompletedTask;
                    }

                    throw callbackException;
                }

                return Task.CompletedTask;
            }, cancellation.Token));

        Assert.True(callbackFailed);
        if (cancel)
            Assert.IsAssignableFrom<OperationCanceledException>(exception);
        else if (failureProgress is 100)
            Assert.Null(exception);
        else
            Assert.Same(callbackException, exception);

        var state = await _client.Indices.GetAsync((Indices)name, cancellationToken: TestCancellationToken);
        Assert.True(state.IsValidResponse, state.GetErrorMessage());
        Assert.Equal(failureProgress is 100 ? targetIndex : name, state.Indices.Keys.Single());
        var physical = state.Indices.Values.Single();
        Assert.False(physical.Settings?.Index?.Blocks?.Write is true);
        Assert.DoesNotContain(ElasticIndexCompatibilityUpgrader.OwnershipAlias, physical.Aliases!.Keys);
        await AssertIndexExistsAsync(targetIndex, failureProgress is 100);
        var document = await _client.GetAsync<Employee>(employee.Id, d => d.Index(name), TestCancellationToken);
        Assert.True(document.Found, document.GetErrorMessage());
        Assert.Equal(employee.Name, document.Source?.Name);
    }
}
