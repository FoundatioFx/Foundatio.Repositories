using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;
using Xunit;
using HttpMethod = Elastic.Transport.HttpMethod;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class PointInTimeQueryTests
{
    private const string AllowedIndex = "pit-query-allowed";
    private const string ExcludedIndex = "pit-query-excluded";

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PointInTime_WhenConfigureQueryReturnsIndexRestriction_UsesOnlyConfiguredIndexes(bool patch)
    {
        using var configuration = new TestConfiguration();
        using var repository = new TestRepository(configuration);
        var query = new RepositoryQuery<TestDocument>();
        var visited = new List<string>();
        var options = new CommandOptions().Cache(false).PageLimit(10);

        if (patch)
        {
            long modified = await repository.PatchAllAsync(query, new ActionPatch<TestDocument>(document =>
            {
                visited.Add(document.Id);
                return false;
            }), options);
            Assert.Equal(0, modified);
        }
        else
        {
            var results = await repository.FindAsync(query, options.SearchAfterPaging(SearchAfterPagingMode.PointInTime));
            visited.AddRange(results.Documents.Select(document => document.Id));
        }

        Assert.Equal(new[] { AllowedIndex }, configuration.Invoker.OpenedIndexes);
        Assert.Equal(new[] { "allowed" }, visited);
        Assert.Equal(1, repository.ConfigureQueryCalls);
        Assert.Empty(query.GetElasticIndexes());
        Assert.Equal(1, configuration.Invoker.ClosedPointInTimes);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PointInTime_WhenDescriptorOverrideSkipsBase_DoesNotReuseIndexesFromFailedQuery(bool cloneOptions)
    {
        using var configuration = new TestConfiguration();
        using var repository = new TestRepository(configuration) { ThrowAfterBaseSearchConfiguration = true };
        var options = new CommandOptions().Cache(false).PageLimit(10).SearchAfterPaging(SearchAfterPagingMode.PointInTime);
        await Assert.ThrowsAsync<InvalidOperationException>(() => repository.FindAsync(new RepositoryQuery<TestDocument>(), options));
        repository.ThrowAfterBaseSearchConfiguration = false;
        repository.SkipBaseSearchConfiguration = true;
        var nextOptions = cloneOptions ? options.Clone() : options;

        var results = await repository.FindAsync(new RepositoryQuery<TestDocument>().Index(ExcludedIndex), nextOptions);

        Assert.Equal(new[] { ExcludedIndex }, configuration.Invoker.OpenedIndexes);
        Assert.Equal("excluded", Assert.Single(results.Documents).Id);
        Assert.Equal(1, repository.ConfigureQueryCalls);
        Assert.Equal(1, configuration.Invoker.ClosedPointInTimes);
    }

    [Theory]
    [InlineData("BeforeQuery", false, false)]
    [InlineData("BeforeQuery", false, true)]
    [InlineData("BeforeQuery", true, false)]
    [InlineData("BeforeQuery", true, true)]
    [InlineData("Descriptor", false, false)]
    [InlineData("Descriptor", false, true)]
    [InlineData("Descriptor", true, false)]
    [InlineData("Descriptor", true, true)]
    [InlineData("AfterQuery", false, false)]
    [InlineData("AfterQuery", false, true)]
    [InlineData("AfterQuery", true, false)]
    [InlineData("AfterQuery", true, true)]
    public async Task PointInTime_WhenSecondPageFails_ClosesOnlyTheOwnedContext(string failureStage, bool callerOwned, bool cloneOptions)
    {
        using var configuration = new TestConfiguration();
        configuration.Invoker.HitsPerIndex = 2;
        using var repository = new TestRepository(configuration);
        var options = new CommandOptions().Cache(false).PageLimit(1).SearchAfterPaging(SearchAfterPagingMode.PointInTime);
        if (callerOwned)
        {
            var opened = await configuration.Client.OpenPointInTimeAsync(AllowedIndex, descriptor => descriptor
                .KeepAlive(TimeSpan.FromMinutes(1).ToElasticDuration()), TestContext.Current.CancellationToken);
            Assert.True(opened.IsValidResponse);
            options.PointInTimeId(opened.Id);
        }

        var results = await repository.FindAsync(new RepositoryQuery<TestDocument>(), options);
        Assert.True(results.HasMore);
        Assert.Single(results.Documents);
        string? pointInTimeId = results.GetPointInTimeId();
        Assert.False(String.IsNullOrEmpty(pointInTimeId));
        Assert.Equal(!callerOwned, options.IsRepoOwnedPointInTime());
        Assert.Equal(0, configuration.Invoker.ClosedPointInTimes);
        repository.CloneOptions = cloneOptions;
        var failure = new InvalidOperationException("The second-page query failed.");
        bool failQuery = true;
        if (failureStage is "BeforeQuery")
            repository.BeforeQuery.AddHandler((_, _) => failQuery ? Task.FromException(failure) : Task.CompletedTask);
        else if (failureStage is "AfterQuery")
            repository.AfterQuery.AddHandler((_, _) => failQuery ? Task.FromException(failure) : Task.CompletedTask);
        else
            repository.SearchConfigurationException = failure;

        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(() => results.NextPageAsync());

        Assert.Same(failure, thrown);
        Assert.Equal(failureStage is "AfterQuery" ? 2 : 1, configuration.Invoker.SearchRequests);
        Assert.False(options.IsRepoOwnedPointInTime());
        if (callerOwned)
        {
            Assert.Empty(configuration.Invoker.ClosedPointInTimeIds);
            Assert.Equal(pointInTimeId, options.GetPointInTimeId());
        }
        else
        {
            Assert.Equal(pointInTimeId, Assert.Single(configuration.Invoker.ClosedPointInTimeIds));
            Assert.Null(options.GetPointInTimeId());
            failQuery = false;
            repository.SearchConfigurationException = null;
            Assert.False(results.HasMore);
            Assert.False(await results.NextPageAsync());
            Assert.Equal(new[] { AllowedIndex }, configuration.Invoker.OpenedIndexes);
            Assert.Equal(failureStage is "AfterQuery" ? 2 : 1, configuration.Invoker.SearchRequests);
            Assert.Equal(pointInTimeId, Assert.Single(configuration.Invoker.ClosedPointInTimeIds));
        }
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task PointInTime_WhenClonedOptionsCompleteSecondPage_ClearsOnlyOwnedContext(bool callerOwned, bool cloneOptions)
    {
        using var configuration = new TestConfiguration();
        configuration.Invoker.HitsPerIndex = 2;
        using var repository = new TestRepository(configuration);
        var options = new CommandOptions().Cache(false).PageLimit(1).SearchAfterPaging(SearchAfterPagingMode.PointInTime);
        if (callerOwned)
        {
            var opened = await configuration.Client.OpenPointInTimeAsync(AllowedIndex, descriptor => descriptor
                .KeepAlive(TimeSpan.FromMinutes(1).ToElasticDuration()), TestContext.Current.CancellationToken);
            Assert.True(opened.IsValidResponse);
            options.PointInTimeId(opened.Id);
        }

        var results = await repository.FindAsync(new RepositoryQuery<TestDocument>(), options);
        Assert.Equal("allowed-0", Assert.Single(results.Documents).Id);
        Assert.True(results.HasMore);
        string? pointInTimeId = results.GetPointInTimeId();
        Assert.False(String.IsNullOrEmpty(pointInTimeId));
        Assert.Equal(!callerOwned, options.IsRepoOwnedPointInTime());
        Assert.Empty(configuration.Invoker.ClosedPointInTimeIds);
        repository.CloneOptions = cloneOptions;

        Assert.True(await results.NextPageAsync());

        Assert.Equal("allowed-1", Assert.Single(results.Documents).Id);
        Assert.False(results.HasMore);
        Assert.False(options.IsRepoOwnedPointInTime());
        if (callerOwned)
        {
            Assert.Empty(configuration.Invoker.ClosedPointInTimeIds);
            Assert.Equal(pointInTimeId, options.GetPointInTimeId());
        }
        else
        {
            Assert.Equal(pointInTimeId, Assert.Single(configuration.Invoker.ClosedPointInTimeIds));
            Assert.Null(options.GetPointInTimeId());
        }
        Assert.False(await results.NextPageAsync());
        Assert.Equal(2, configuration.Invoker.SearchRequests);
        Assert.Equal(callerOwned ? 0 : 1, configuration.Invoker.ClosedPointInTimes);
    }

    [Theory]
    [InlineData("Stop", false)]
    [InlineData("Stop", true)]
    [InlineData("Throw", false)]
    [InlineData("Throw", true)]
    [InlineData("Complete", false)]
    [InlineData("Complete", true)]
    public async Task BatchProcessAsync_WithInitiallyClonedOptions_ClosesOnlyOwnedPointInTime(string completion, bool callerOwned)
    {
        // Arrange
        using var configuration = new TestConfiguration();
        configuration.Invoker.HitsPerIndex = 2;
        using var repository = new TestRepository(configuration) { CloneOptions = true };
        var options = new CommandOptions().Cache(false).PageLimit(1).SearchAfterPaging(SearchAfterPagingMode.PointInTime);
        if (callerOwned)
        {
            var opened = await configuration.Client.OpenPointInTimeAsync(AllowedIndex, descriptor => descriptor
                .KeepAlive(TimeSpan.FromMinutes(1).ToElasticDuration()), TestContext.Current.CancellationToken);
            Assert.True(opened.IsValidResponse);
            options.PointInTimeId(opened.Id);
        }

        var failure = new InvalidOperationException("The batch callback failed.");
        var visited = new List<string>();
        string? pointInTimeId = null;

        // Act
        var processing = repository.BatchProcessAsync(new RepositoryQuery<TestDocument>(), results =>
        {
            pointInTimeId = results.GetPointInTimeId();
            visited.AddRange(results.Documents.Select(document => document.Id));
            return completion is "Throw" ? Task.FromException<bool>(failure) : Task.FromResult(completion is "Complete");
        }, options);
        if (completion is "Throw")
            Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => processing));
        else
            Assert.Equal(completion is "Complete" ? 2 : 0, await processing);

        // Assert
        Assert.False(String.IsNullOrEmpty(pointInTimeId));
        Assert.Equal(completion is "Complete" ? new[] { "allowed-0", "allowed-1" } : new[] { "allowed-0" }, visited);
        Assert.Equal(completion is "Complete" ? 2 : 1, configuration.Invoker.SearchRequests);
        if (callerOwned)
        {
            Assert.Empty(configuration.Invoker.ClosedPointInTimeIds);
            Assert.Equal(pointInTimeId, options.GetPointInTimeId());
        }
        else
        {
            Assert.Equal(pointInTimeId, Assert.Single(configuration.Invoker.ClosedPointInTimeIds));
            Assert.Null(options.GetPointInTimeId());
        }
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task PatchAllAsync_WhenSecondPagePointInTimeDisappears_ThrowsAfterNotifyingCompletedWrites(bool callerOwned, bool cloneOptions)
    {
        // Arrange
        using var configuration = new TestConfiguration();
        configuration.Invoker.HitsPerIndex = 2;
        configuration.Invoker.FailSearchAt = 2;
        using var repository = new TestRepository(configuration) { CloneOptions = cloneOptions };
        var completedIds = new List<string>();
        var changedIds = new List<string>();
        var options = new CommandOptions().Cache(false).PageLimit(1).UpdateCallback(completedIds.AddRange);
        if (callerOwned)
        {
            var opened = await configuration.Client.OpenPointInTimeAsync(AllowedIndex, descriptor => descriptor
                .KeepAlive(TimeSpan.FromMinutes(1).ToElasticDuration()), TestContext.Current.CancellationToken);
            Assert.True(opened.IsValidResponse);
            options.PointInTimeId(opened.Id);
        }
        repository.DocumentsChanged.AddHandler((_, args) =>
        {
            changedIds.AddRange(args.Documents.Select(document => document.Value.Id));
            return Task.CompletedTask;
        });

        // Act
        await Assert.ThrowsAsync<DocumentException>(() => repository.PatchAllAsync(new RepositoryQuery<TestDocument>(),
            new ActionPatch<TestDocument>(document =>
            {
                document.Value = "after";
            }), options));

        // Assert
        Assert.Equal(new[] { "allowed-0" }, configuration.Invoker.WrittenIds);
        Assert.Equal(new[] { "allowed-0" }, changedIds);
        Assert.Equal(new[] { "allowed-0" }, completedIds);
        Assert.Equal(2, configuration.Invoker.SearchRequests);
        Assert.Equal(new[] { AllowedIndex }, configuration.Invoker.OpenedIndexes);
        Assert.Equal(callerOwned ? Array.Empty<string>() : new[] { "point-in-time" }, configuration.Invoker.ClosedPointInTimeIds);
        Assert.Equal(callerOwned ? "point-in-time" : null, options.GetPointInTimeId());
    }

    [Theory]
    [InlineData("Timeout", 1)]
    [InlineData("Timeout", 2)]
    [InlineData("ShardFailure", 1)]
    [InlineData("ShardFailure", 2)]
    public async Task PatchAllAsync_WithIncompleteSelectionPage_ThrowsBeforeWritingThatPage(string incompleteType, int incompletePage)
    {
        // Arrange
        using var configuration = new TestConfiguration();
        configuration.Invoker.HitsPerIndex = 3;
        configuration.Invoker.IncompleteSearchAt = incompletePage;
        configuration.Invoker.IncompleteSearchType = incompleteType;
        using var repository = new TestRepository(configuration);
        var completedIds = new List<string>();
        var changedIds = new List<string>();
        var options = new CommandOptions().Cache(false).PageLimit(1).UpdateCallback(completedIds.AddRange);
        repository.DocumentsChanged.AddHandler((_, args) =>
        {
            changedIds.AddRange(args.Documents.Select(document => document.Value.Id));
            return Task.CompletedTask;
        });

        // Act
        await Assert.ThrowsAsync<DocumentException>(() => repository.PatchAllAsync(new RepositoryQuery<TestDocument>(),
            new ActionPatch<TestDocument>(document =>
            {
                document.Value = "after";
            }), options));

        // Assert
        string[] expectedCompletedIds = incompletePage is 1 ? [] : ["allowed-0"];
        Assert.Equal(expectedCompletedIds, configuration.Invoker.WrittenIds);
        Assert.Equal(expectedCompletedIds, changedIds);
        Assert.Equal(expectedCompletedIds, completedIds);
        Assert.Equal(incompletePage, configuration.Invoker.SearchRequests);
        Assert.Equal(new[] { AllowedIndex }, configuration.Invoker.OpenedIndexes);
        Assert.Equal(new[] { "point-in-time" }, configuration.Invoker.ClosedPointInTimeIds);
    }

    [Theory]
    [InlineData("Timeout")]
    [InlineData("ShardFailure")]
    public async Task FindAsync_WithIncompleteResponse_PreservesOrdinaryReadBehavior(string incompleteType)
    {
        // Arrange
        using var configuration = new TestConfiguration();
        configuration.Invoker.HitsPerIndex = 3;
        configuration.Invoker.IncompleteSearchAt = 1;
        configuration.Invoker.IncompleteSearchType = incompleteType;
        using var repository = new TestRepository(configuration);

        // Act
        var results = await repository.FindAsync(new RepositoryQuery<TestDocument>(), new CommandOptions().Cache(false).PageLimit(10));

        // Assert
        Assert.Equal("allowed-0", Assert.Single(results.Documents).Id);
        Assert.False(results.HasMore);
        Assert.Equal(1, configuration.Invoker.SearchRequests);
        Assert.Empty(configuration.Invoker.WrittenIds);
        Assert.Empty(configuration.Invoker.OpenedIndexes);
        Assert.Empty(configuration.Invoker.ClosedPointInTimeIds);
    }

    [Fact]
    public async Task FindAsync_WithMissingIndexAndNoPointInTime_ReturnsEmptyResults()
    {
        // Arrange
        using var configuration = new TestConfiguration();
        configuration.Invoker.FailSearchAt = 1;
        configuration.Invoker.SearchFailureType = "index_not_found_exception";
        using var repository = new TestRepository(configuration);

        // Act
        var results = await repository.FindAsync(new RepositoryQuery<TestDocument>(), new CommandOptions().Cache(false));

        // Assert
        Assert.Empty(results.Hits);
        Assert.False(results.HasMore);
        Assert.Equal(1, configuration.Invoker.SearchRequests);
        Assert.Empty(configuration.Invoker.OpenedIndexes);
        Assert.Empty(configuration.Invoker.ClosedPointInTimeIds);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task BatchProcessAsync_WithRotatedPointInTimeIds_ClosesLatestOwnedId(bool cloneOptions, bool stopAfterFirstPage)
    {
        // Arrange
        using var configuration = new TestConfiguration();
        configuration.Invoker.HitsPerIndex = 2;
        configuration.Invoker.RotatePointInTimeIds = true;
        using var repository = new TestRepository(configuration) { CloneOptions = cloneOptions };
        var options = new CommandOptions().Cache(false).PageLimit(1).SearchAfterPaging(SearchAfterPagingMode.PointInTime);
        var visited = new List<string>();

        // Act
        long processed = await repository.BatchProcessAsync(new RepositoryQuery<TestDocument>(), results =>
        {
            visited.AddRange(results.Documents.Select(document => document.Id));
            return Task.FromResult(!stopAfterFirstPage);
        }, options);

        // Assert
        Assert.Equal(stopAfterFirstPage ? 0 : 2, processed);
        Assert.Equal(stopAfterFirstPage ? new[] { "allowed-0" } : new[] { "allowed-0", "allowed-1" }, visited);
        Assert.Equal(stopAfterFirstPage ? new[] { "point-in-time" } : new[] { "point-in-time", "point-in-time-1" }, configuration.Invoker.SearchPointInTimeIds);
        Assert.Equal(stopAfterFirstPage ? "point-in-time-1" : "point-in-time-2", Assert.Single(configuration.Invoker.ClosedPointInTimeIds));
        Assert.Null(options.GetPointInTimeId());
    }

    [Theory]
    [InlineData("Timeout", 1)]
    [InlineData("Timeout", 2)]
    [InlineData("ShardFailure", 1)]
    [InlineData("ShardFailure", 2)]
    public async Task PatchAllAsync_WithRotatedIncompleteSelectionPage_ClosesNewestContext(string incompleteType, int incompletePage)
    {
        // Arrange
        using var configuration = new TestConfiguration();
        configuration.Invoker.HitsPerIndex = 3;
        configuration.Invoker.RotatePointInTimeIds = true;
        configuration.Invoker.IncompleteSearchAt = incompletePage;
        configuration.Invoker.IncompleteSearchType = incompleteType;
        using var repository = new TestRepository(configuration) { CloneOptions = true };
        var completedIds = new List<string>();
        var options = new CommandOptions().Cache(false).PageLimit(1).UpdateCallback(completedIds.AddRange);

        // Act
        await Assert.ThrowsAsync<DocumentException>(() => repository.PatchAllAsync(new RepositoryQuery<TestDocument>(),
            new ActionPatch<TestDocument>(document =>
            {
                document.Value = "after";
            }), options));

        // Assert
        string[] expectedCompletedIds = incompletePage is 1 ? [] : ["allowed-0"];
        Assert.Equal(expectedCompletedIds, configuration.Invoker.WrittenIds);
        Assert.Equal(expectedCompletedIds, completedIds);
        Assert.Equal(incompletePage is 1 ? new[] { "point-in-time" } : new[] { "point-in-time", "point-in-time-1" }, configuration.Invoker.SearchPointInTimeIds);
        Assert.Equal($"point-in-time-{incompletePage}", Assert.Single(configuration.Invoker.ClosedPointInTimeIds));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PointInTime_WhenCallerReplacesIdOnClone_KeepsOriginalOwnership(bool cloneOptions)
    {
        // Arrange
        using var configuration = new TestConfiguration();
        configuration.Invoker.HitsPerIndex = 2;
        configuration.Invoker.RotatePointInTimeIds = true;
        using var repository = new TestRepository(configuration) { CloneOptions = cloneOptions };
        var options = new CommandOptions().Cache(false).PageLimit(1).SearchAfterPaging(SearchAfterPagingMode.PointInTime);
        var results = await repository.FindAsync(new RepositoryQuery<TestDocument>(), options);
        Assert.True(results.HasMore);

        // Act
        var callerOptions = options.Clone().PointInTimeId("caller-replacement");
        Assert.True(options.IsRepoOwnedPointInTime());
        Assert.False(callerOptions.IsRepoOwnedPointInTime());
        Assert.True(await results.NextPageAsync());

        // Assert
        Assert.Equal("allowed-1", Assert.Single(results.Documents).Id);
        Assert.False(results.HasMore);
        Assert.False(options.IsRepoOwnedPointInTime());
        Assert.Null(options.GetPointInTimeId());
        Assert.Equal("caller-replacement", callerOptions.GetPointInTimeId());
        Assert.Equal(new[] { "point-in-time", "point-in-time-1" }, configuration.Invoker.SearchPointInTimeIds);
        Assert.Equal("point-in-time-2", Assert.Single(configuration.Invoker.ClosedPointInTimeIds));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FindAsync_WhenAfterQueryReplacesIncomingPointInTime_KeepsCallerReplacement(bool failQuery)
    {
        // Arrange
        using var configuration = new TestConfiguration();
        using var repository = new TestRepository(configuration) { CloneOptions = true };
        var options = new CommandOptions().Cache(false).PageLimit(10).SearchAfterPaging(SearchAfterPagingMode.PointInTime);
        var failure = new InvalidOperationException("AfterQuery failed after the caller replaced its context.");
        repository.AfterQuery.AddHandler((_, _) =>
        {
            options.PointInTimeId("caller-replacement");
            return failQuery ? Task.FromException(failure) : Task.CompletedTask;
        });

        // Act
        if (failQuery)
            Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => repository.FindAsync(new RepositoryQuery<TestDocument>(), options)));
        else
            Assert.Equal("allowed", Assert.Single((await repository.FindAsync(new RepositoryQuery<TestDocument>(), options)).Documents).Id);

        // Assert
        Assert.Equal("point-in-time", Assert.Single(configuration.Invoker.ClosedPointInTimeIds));
        Assert.Equal("caller-replacement", options.GetPointInTimeId());
        Assert.False(options.IsRepoOwnedPointInTime());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PatchAllAsync_WhenFinalPointInTimeCloseThrows_PatchesEveryFetchedPage(bool cloneOptions)
    {
        // Arrange
        using var configuration = new TestConfiguration();
        configuration.Invoker.HitsPerIndex = 2;
        configuration.Invoker.CloseException = new InvalidOperationException("The close request failed.");
        using var repository = new TestRepository(configuration) { CloneOptions = cloneOptions };
        var completedIds = new List<string>();

        // Act
        long patched = await repository.PatchAllAsync(new RepositoryQuery<TestDocument>(),
            new ActionPatch<TestDocument>(document =>
            {
                document.Value = "after";
            }), new CommandOptions().Cache(false).PageLimit(1).UpdateCallback(completedIds.AddRange));

        // Assert
        Assert.Equal(2, patched);
        Assert.Equal(new[] { "allowed-0", "allowed-1" }, configuration.Invoker.WrittenIds);
        Assert.Equal(new[] { "allowed-0", "allowed-1" }, completedIds);
        Assert.Equal(2, configuration.Invoker.SearchRequests);
        Assert.Equal("point-in-time", Assert.Single(configuration.Invoker.ClosedPointInTimeIds));
    }

    [Theory]
    [InlineData("Timeout")]
    [InlineData("ShardFailure")]
    public async Task FindAsync_WithOptionsReusedAfterBatchProcessing_PreservesOrdinaryPartialReads(string incompleteType)
    {
        // Arrange
        using var configuration = new TestConfiguration();
        configuration.Invoker.HitsPerIndex = 2;
        using var repository = new TestRepository(configuration);
        var options = new CommandOptions().Cache(false).PageLimit(1);
        Assert.Equal(2, await repository.BatchProcessAsync(new RepositoryQuery<TestDocument>(), _ => Task.FromResult(true), options));
        configuration.Invoker.IncompleteSearchAt = configuration.Invoker.SearchRequests + 1;
        configuration.Invoker.IncompleteSearchType = incompleteType;
        options.SearchAfter().SearchAfterPaging(false).PageNumber(1).PageLimit(10);

        // Act
        var results = await repository.FindAsync(new RepositoryQuery<TestDocument>(), options);

        // Assert
        Assert.Equal("allowed-0", Assert.Single(results.Documents).Id);
        Assert.False(results.HasMore);
        Assert.Equal(3, configuration.Invoker.SearchRequests);
        Assert.Empty(configuration.Invoker.WrittenIds);
    }

    [Fact]
    public async Task BatchProcessAsync_WithCachedOrdinaryPartialResult_ValidatesFreshSelectionBeforeProcessing()
    {
        // Arrange
        using var configuration = new TestConfiguration();
        configuration.Invoker.HitsPerIndex = 3;
        configuration.Invoker.IncompleteSearchAt = 1;
        configuration.Invoker.IncompleteSearchType = "Timeout";
        using var repository = new TestRepository(configuration);
        var options = new CommandOptions().Cache("partial-selection").PageLimit(10);
        var query = new RepositoryQuery<TestDocument>();
        Assert.Equal("allowed-0", Assert.Single((await repository.FindAsync(query, options)).Documents).Id);
        Assert.Equal("allowed-0", Assert.Single((await repository.FindAsync(query, options)).Documents).Id);
        Assert.Equal(1, configuration.Invoker.SearchRequests);
        configuration.Invoker.IncompleteSearchAt = 2;
        var processedIds = new List<string>();

        // Act
        await Assert.ThrowsAsync<DocumentException>(() => repository.BatchProcessAsync(query, results =>
        {
            processedIds.AddRange(results.Documents.Select(document => document.Id));
            return Task.FromResult(true);
        }, options));

        // Assert
        Assert.Empty(processedIds);
        Assert.Equal(2, configuration.Invoker.SearchRequests);
        Assert.Empty(configuration.Invoker.WrittenIds);
    }

    [Fact]
    public async Task PatchAllAsync_WhenScrollContextDisappears_ThrowsAfterNotifyingCompletedWrites()
    {
        // Arrange
        using var configuration = new TestConfiguration();
        configuration.Invoker.HitsPerIndex = 2;
        configuration.Invoker.MissingScrollContext = true;
        using var repository = new TestRepository(configuration);
        var completedIds = new List<string>();
        var changedIds = new List<string>();
        repository.DocumentsChanged.AddHandler((_, args) =>
        {
            changedIds.AddRange(args.Documents.Select(document => document.Value.Id));
            return Task.CompletedTask;
        });

        // Act
        await Assert.ThrowsAsync<DocumentException>(() => repository.PatchAllAsync(new RepositoryQuery<TestDocument>(),
            new ActionPatch<TestDocument>(document =>
            {
                document.Value = "after";
            }), new CommandOptions().Cache(false).PageLimit(1).SnapshotPaging().UpdateCallback(completedIds.AddRange)));

        // Assert
        Assert.Equal(new[] { "allowed-0" }, configuration.Invoker.WrittenIds);
        Assert.Equal(new[] { "allowed-0" }, changedIds);
        Assert.Equal(new[] { "allowed-0" }, completedIds);
        Assert.Equal(1, configuration.Invoker.SearchRequests);
        Assert.Equal(1, configuration.Invoker.ScrollRequests);
        Assert.Empty(configuration.Invoker.OpenedIndexes);
        Assert.Empty(configuration.Invoker.ClosedPointInTimeIds);
    }

    [Theory]
    [InlineData("Timeout", 1)]
    [InlineData("Timeout", 2)]
    [InlineData("ShardFailure", 1)]
    [InlineData("ShardFailure", 2)]
    public async Task PatchAllAsync_WithIncompleteScrollPage_ClearsLatestContextAfterPriorWriteEffects(string incompleteType, int incompletePage)
    {
        // Arrange
        using var configuration = new TestConfiguration();
        configuration.Invoker.HitsPerIndex = 3;
        configuration.Invoker.IncompleteSearchAt = incompletePage;
        configuration.Invoker.IncompleteSearchType = incompleteType;
        using var repository = new TestRepository(configuration);
        var completedIds = new List<string>();
        var changedIds = new List<string>();
        FindResults<TestDocument>? selection = null;
        repository.AfterQuery.AddHandler((_, args) =>
        {
            selection = (FindResults<TestDocument>)args.Result;
            return Task.CompletedTask;
        });
        repository.DocumentsChanged.AddHandler((_, args) =>
        {
            changedIds.AddRange(args.Documents.Select(document => document.Value.Id));
            return Task.CompletedTask;
        });

        // Act
        await Assert.ThrowsAsync<DocumentException>(() => repository.PatchAllAsync(new RepositoryQuery<TestDocument>(),
            new ActionPatch<TestDocument>(document =>
            {
                document.Value = "after";
            }), new CommandOptions().Cache(false).PageLimit(1).SnapshotPaging().UpdateCallback(completedIds.AddRange)));

        // Assert
        string[] expectedCompletedIds = incompletePage is 1 ? [] : ["allowed-0"];
        Assert.Equal(expectedCompletedIds, configuration.Invoker.WrittenIds);
        Assert.Equal(expectedCompletedIds, changedIds);
        Assert.Equal(expectedCompletedIds, completedIds);
        if (incompletePage is 2)
        {
            Assert.NotNull(selection);
            Assert.False(selection.HasMore);
            Assert.False(await selection.NextPageAsync());
        }
        Assert.Equal(1, configuration.Invoker.SearchRequests);
        Assert.Equal(incompletePage - 1, configuration.Invoker.ScrollRequests);
        Assert.Equal($"scroll-context-{incompletePage}", Assert.Single(configuration.Invoker.ClearedScrollIds));
        Assert.Empty(configuration.Invoker.ClosedPointInTimeIds);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task BatchProcessAsync_WhenScrollCallbackStops_ClearsLatestContext(bool throwFromCallback)
    {
        // Arrange
        using var configuration = new TestConfiguration();
        configuration.Invoker.HitsPerIndex = 3;
        using var repository = new TestRepository(configuration);
        var visited = new List<string>();
        var failure = new InvalidOperationException("The second scroll batch callback failed.");

        // Act
        var processing = repository.BatchProcessAsync(new RepositoryQuery<TestDocument>(), results =>
        {
            visited.AddRange(results.Documents.Select(document => document.Id));
            if (visited.Count is 1)
                return Task.FromResult(true);

            return throwFromCallback ? Task.FromException<bool>(failure) : Task.FromResult(false);
        }, new CommandOptions().Cache(false).PageLimit(1).SnapshotPaging());
        if (throwFromCallback)
            Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => processing));
        else
            Assert.Equal(1, await processing);

        // Assert
        Assert.Equal(new[] { "allowed-0", "allowed-1" }, visited);
        Assert.Equal(new[] { "scroll-context-1" }, configuration.Invoker.RequestedScrollIds);
        Assert.Equal("scroll-context-2", Assert.Single(configuration.Invoker.ClearedScrollIds));
        Assert.Empty(configuration.Invoker.ClosedPointInTimeIds);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PatchAllAsync_WhenFinalScrollPageClearsLatestContext_PreservesFetchedWrites(bool clearThrows)
    {
        // Arrange
        using var configuration = new TestConfiguration();
        configuration.Invoker.HitsPerIndex = 3;
        if (clearThrows)
            configuration.Invoker.ClearScrollException = new InvalidOperationException("The scroll clear request failed.");
        using var repository = new TestRepository(configuration);
        var completedIds = new List<string>();
        var changedIds = new List<string>();
        repository.DocumentsChanged.AddHandler((_, args) =>
        {
            changedIds.AddRange(args.Documents.Select(document => document.Value.Id));
            return Task.CompletedTask;
        });

        // Act
        long patched = await repository.PatchAllAsync(new RepositoryQuery<TestDocument>(),
            new ActionPatch<TestDocument>(document =>
            {
                document.Value = "after";
            }), new CommandOptions().Cache(false).PageLimit(2).SnapshotPaging().UpdateCallback(completedIds.AddRange));

        // Assert
        Assert.Equal(3, patched);
        Assert.Equal(new[] { "allowed-0", "allowed-1", "allowed-2" }, configuration.Invoker.WrittenIds);
        Assert.Equal(configuration.Invoker.WrittenIds, changedIds);
        Assert.Equal(configuration.Invoker.WrittenIds, completedIds);
        Assert.Equal(1, configuration.Invoker.SearchRequests);
        Assert.Equal(new[] { "scroll-context-1" }, configuration.Invoker.RequestedScrollIds);
        Assert.Equal("scroll-context-2", Assert.Single(configuration.Invoker.ClearedScrollIds));
        Assert.Empty(configuration.Invoker.ClosedPointInTimeIds);
    }

    public sealed class TestDocument : IIdentity
    {
        public string Id { get; set; } = null!;
        public string? Value { get; set; }
    }

    private sealed class TestRepository(TestConfiguration configuration) : ElasticRepositoryBase<TestDocument>(new TestIndex(configuration))
    {
        public int ConfigureQueryCalls { get; private set; }
        public bool CloneOptions { get; set; }
        public bool SkipBaseSearchConfiguration { get; set; }
        public bool ThrowAfterBaseSearchConfiguration { get; set; }
        public Exception? SearchConfigurationException { get; set; }

        protected override ICommandOptions<TestDocument> ConfigureOptions(ICommandOptions<TestDocument>? options)
        {
            return base.ConfigureOptions(CloneOptions ? options?.Clone() : options);
        }

        protected override IRepositoryQuery ConfigureQuery(IRepositoryQuery<TestDocument> query)
        {
            ConfigureQueryCalls++;
            var configured = query.Clone<RepositoryQuery<TestDocument>>().Index(AllowedIndex);
            return base.ConfigureQuery(configured);
        }

        protected override async Task<SearchRequestDescriptor<TestDocument>> ConfigureSearchDescriptorAsync(SearchRequestDescriptor<TestDocument> search, IRepositoryQuery query, ICommandOptions options)
        {
            if (SkipBaseSearchConfiguration)
                return search.Size(options.GetLimit() + 1);

            var configured = await base.ConfigureSearchDescriptorAsync(search, query, options);
            if (ThrowAfterBaseSearchConfiguration)
                throw new InvalidOperationException("The descriptor override failed after base configuration.");
            if (SearchConfigurationException is { } failure)
                throw failure;

            return configured;
        }
    }

    private sealed class TestIndex : Index<TestDocument>
    {
        public TestIndex(TestConfiguration configuration) : base(configuration, "pit-query-tests")
        {
            HasMultipleIndexes = true;
        }

        public override string[] GetIndexesByQuery(IRepositoryQuery query)
        {
            var indexes = query.GetElasticIndexes();
            return indexes.Count > 0 ? indexes.ToArray() : [AllowedIndex, ExcludedIndex];
        }
    }

    private sealed class TestConfiguration : ElasticConfiguration
    {
        public PointInTimeInvoker Invoker { get; } = new();
        protected override IRequestInvoker CreateRequestInvoker() => Invoker;
    }

    private sealed class PointInTimeInvoker : IRequestInvoker
    {
        private readonly InMemoryRequestInvoker _owner = new();
        private string[] _currentIndexes = [];
        private object[] _scrollHits = [];
        private int _scrollPageSize;
        private int _scrollOffset;
        private static readonly Dictionary<string, IEnumerable<string>> _headers = new(StringComparer.OrdinalIgnoreCase) { ["X-Elastic-Product"] = ["Elasticsearch"] };
        public ResponseFactory ResponseFactory => _owner.ResponseFactory;
        public List<string> OpenedIndexes { get; } = [];
        public List<string> ClosedPointInTimeIds { get; } = [];
        public List<string> SearchPointInTimeIds { get; } = [];
        public List<string> RequestedScrollIds { get; } = [];
        public List<string> ClearedScrollIds { get; } = [];
        public List<string> WrittenIds { get; } = [];
        public int ClosedPointInTimes => ClosedPointInTimeIds.Count;
        public int SearchRequests { get; private set; }
        public int ScrollRequests { get; private set; }
        public int HitsPerIndex { get; set; } = 1;
        public bool RotatePointInTimeIds { get; set; }
        public Exception? CloseException { get; set; }
        public Exception? ClearScrollException { get; set; }
        public bool MissingScrollContext { get; set; }
        public int FailSearchAt { get; set; }
        public string SearchFailureType { get; set; } = "search_context_missing_exception";
        public int IncompleteSearchAt { get; set; }
        public string? IncompleteSearchType { get; set; }

        public TResponse Request<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData)
            where TResponse : TransportResponse, new()
        {
            object response;
            int status = 200;
            if (endpoint.PathAndQuery.Contains("_mapping", StringComparison.Ordinal))
            {
                response = new Dictionary<string, object>
                {
                    ["pit-query-tests"] = new
                    {
                        mappings = new { properties = new { id = new { type = "keyword" }, value = new { type = "keyword" } } }
                    }
                };
            }
            else if (endpoint.PathAndQuery.Contains("_pit", StringComparison.Ordinal))
            {
                if (endpoint.Method is HttpMethod.DELETE)
                {
                    Assert.NotNull(postData);
                    using var requestStream = new MemoryStream();
                    postData.Write(requestStream, boundConfiguration.ConnectionSettings, false);
                    using var request = JsonDocument.Parse(requestStream.ToArray());
                    ClosedPointInTimeIds.Add(request.RootElement.GetProperty("id").GetString()!);
                    if (CloseException is { } failure)
                        throw failure;
                    response = new { succeeded = true, num_freed = 1 };
                }
                else
                {
                    string path = endpoint.PathAndQuery.Split('?', 2)[0];
                    _currentIndexes = Uri.UnescapeDataString(path.Trim('/').Split('/')[0]).Split(',');
                    OpenedIndexes.AddRange(_currentIndexes);
                    response = new { id = "point-in-time" };
                }
            }
            else if (endpoint.PathAndQuery.Contains("_bulk", StringComparison.Ordinal))
            {
                Assert.NotNull(postData);
                using var requestStream = new MemoryStream();
                postData.Write(requestStream, boundConfiguration.ConnectionSettings, false);
                var lines = Encoding.UTF8.GetString(requestStream.ToArray()).Split('\n', StringSplitOptions.RemoveEmptyEntries);
                var items = lines.Where((_, line) => line % 2 is 0).Select(line =>
                {
                    using var operation = JsonDocument.Parse(line);
                    var metadata = operation.RootElement.GetProperty("index");
                    string id = metadata.GetProperty("_id").GetString()!;
                    WrittenIds.Add(id);
                    return new { index = new { _index = metadata.GetProperty("_index").GetString(), _id = id, status = 200, result = "updated", _seq_no = 1, _primary_term = 1 } };
                }).ToArray();
                response = new { took = 1, errors = false, items };
            }
            else if (endpoint.PathAndQuery.Contains("_search/scroll", StringComparison.Ordinal))
            {
                Assert.NotNull(postData);
                using var requestStream = new MemoryStream();
                postData.Write(requestStream, boundConfiguration.ConnectionSettings, false);
                using var request = JsonDocument.Parse(requestStream.ToArray());
                var scrollIds = request.RootElement.GetProperty("scroll_id");
                if (endpoint.Method is HttpMethod.DELETE)
                {
                    if (scrollIds.ValueKind is JsonValueKind.Array)
                        ClearedScrollIds.AddRange(scrollIds.EnumerateArray().Select(id => id.GetString()!));
                    else
                        ClearedScrollIds.Add(scrollIds.GetString()!);
                    if (ClearScrollException is { } failure)
                        throw failure;
                    response = new { succeeded = true, num_freed = 1 };
                }
                else
                {
                    ScrollRequests++;
                    RequestedScrollIds.Add(scrollIds.GetString()!);
                    if (MissingScrollContext)
                    {
                        status = 404;
                        response = new { error = new { type = "search_context_missing_exception", reason = "No search context found for id [1]" }, status };
                    }
                    else
                    {
                        var hits = _scrollHits.Skip(_scrollOffset).Take(_scrollPageSize).ToArray();
                        _scrollOffset += hits.Length;
                        bool incomplete = ScrollRequests + 1 == IncompleteSearchAt;
                        if (incomplete)
                            hits = hits.Take(1).ToArray();
                        bool timedOut = incomplete && IncompleteSearchType is "Timeout";
                        int failedShards = incomplete && IncompleteSearchType is "ShardFailure" ? 1 : 0;
                        response = new
                        {
                            took = 1,
                            timed_out = timedOut,
                            _shards = new { total = 2, successful = 2 - failedShards, skipped = 0, failed = failedShards },
                            _scroll_id = $"scroll-context-{ScrollRequests + 1}",
                            hits = new { total = new { value = _scrollHits.Length, relation = "eq" }, hits }
                        };
                    }
                }
            }
            else
            {
                Assert.Contains("_search", endpoint.PathAndQuery, StringComparison.Ordinal);
                SearchRequests++;
                Assert.NotNull(postData);
                using var requestStream = new MemoryStream();
                postData.Write(requestStream, boundConfiguration.ConnectionSettings, false);
                using var request = JsonDocument.Parse(requestStream.ToArray());
                string? searchAfter = request.RootElement.TryGetProperty("search_after", out var sortValues)
                    ? sortValues[0].GetString()
                    : null;
                string? pointInTimeId = request.RootElement.TryGetProperty("pit", out var pit) ? pit.GetProperty("id").GetString() : null;
                if (pointInTimeId is not null)
                    SearchPointInTimeIds.Add(pointInTimeId);
                string searchPath = endpoint.PathAndQuery.Split('?', 2)[0];
                var indexes = pointInTimeId is not null
                    ? _currentIndexes
                    : Uri.UnescapeDataString(searchPath.Trim('/').Split('/')[0]).Split(',');
                var hits = indexes.SelectMany(index => Enumerable.Range(0, HitsPerIndex).Select(ordinal =>
                {
                    string id = index is AllowedIndex ? "allowed" : "excluded";
                    if (HitsPerIndex > 1)
                        id = $"{id}-{ordinal}";
                    return new { _index = index, _id = id, _source = new { id, value = "before" }, sort = new[] { id } };
                })).Where(hit => searchAfter is null || String.CompareOrdinal(hit._id, searchAfter) > 0).ToArray();
                string? scrollId = endpoint.PathAndQuery.Contains("scroll=", StringComparison.Ordinal) ? "scroll-context-1" : null;
                if (scrollId is not null)
                {
                    _scrollHits = hits.Cast<object>().ToArray();
                    _scrollPageSize = request.RootElement.GetProperty("size").GetInt32();
                    hits = hits.Take(_scrollPageSize).ToArray();
                    _scrollOffset = hits.Length;
                }
                bool incomplete = SearchRequests == IncompleteSearchAt;
                if (incomplete)
                    hits = hits.Take(1).ToArray();
                bool timedOut = incomplete && IncompleteSearchType is "Timeout";
                int failedShards = incomplete && IncompleteSearchType is "ShardFailure" ? 1 : 0;
                response = new
                {
                    took = 1,
                    timed_out = timedOut,
                    _shards = new { total = 2, successful = 2 - failedShards, skipped = 0, failed = failedShards },
                    pit_id = pointInTimeId is not null && RotatePointInTimeIds ? $"point-in-time-{SearchRequests}" : pointInTimeId,
                    _scroll_id = scrollId,
                    hits = new { total = new { value = indexes.Length * HitsPerIndex, relation = "eq" }, hits }
                };
                if (SearchRequests == FailSearchAt)
                {
                    status = 404;
                    response = new
                    {
                        error = new
                        {
                            type = SearchFailureType,
                            reason = SearchFailureType is "index_not_found_exception" ? "no such index" : "No search context found for id [1]"
                        },
                        status
                    };
                }
            }

            byte[] bytes = JsonSerializer.SerializeToUtf8Bytes(response);
            using var stream = new MemoryStream(bytes);
            return ResponseFactory.Create<TResponse>(endpoint, boundConfiguration, postData, null, status, _headers, stream, "application/json", bytes.Length, null, null);
        }

        public Task<TResponse> RequestAsync<TResponse>(Endpoint endpoint, BoundConfiguration boundConfiguration, PostData? postData, CancellationToken cancellationToken)
            where TResponse : TransportResponse, new()
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(Request<TResponse>(endpoint, boundConfiguration, postData));
        }

        public void Dispose() => ((IDisposable)_owner).Dispose();
    }
}
