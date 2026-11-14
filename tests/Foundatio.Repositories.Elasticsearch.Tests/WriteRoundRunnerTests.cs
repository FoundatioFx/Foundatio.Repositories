using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.Repositories.Exceptions;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class WriteRoundRunnerTests
{
    private sealed record Item(DocumentKey Key, int Attempt = 0) : IWriteItem
    {
        public ElasticDocumentVersion Version => new(1, Attempt);
    }

    private static WriteRoundRunner CreateRunner() => new(NullLogger.Instance, TimeProvider.System);

    [Fact]
    public async Task RunAsync_WithSameIdInTwoIndexes_GivesEachItemItsOwnOutcome()
    {
        // Arrange
        var items = new[] { new Item(new DocumentKey("a", "1")), new Item(new DocumentKey("b", "1")) };

        // Act
        var run = await CreateRunner().RunAsync<Item>("Test", items, 3, (pending, _) => Task.FromResult<IReadOnlyList<WriteItemResult>>(
            pending.Select(p => new WriteItemResult(p.Key.Index is "a" ? WriteOutcome.Succeeded : WriteOutcome.Fatal)).ToList()), null, CancellationToken.None);

        // Assert
        Assert.Equal([WriteOutcome.Succeeded, WriteOutcome.Fatal], run.Items.Select(i => i.Result.Outcome));
        Assert.Equal(1, run.Rounds);
    }

    [Fact]
    public async Task RunAsync_WithTransientAndConflicts_RetriesWithinBudgetAndEndsEveryItem()
    {
        // Arrange
        var items = Enumerable.Range(0, 4).Select(i => new Item(new DocumentKey("i", i.ToString()))).ToList();
        int rounds = 0;

        // Act
        var run = await CreateRunner().RunAsync<Item>("Test", items, 2, (pending, _) =>
        {
            rounds++;
            return Task.FromResult<IReadOnlyList<WriteItemResult>>(pending.Select(p => new WriteItemResult(p.Key.Id switch
            {
                "0" => WriteOutcome.Succeeded,
                "1" => p.Attempt >= 1 ? WriteOutcome.Succeeded : WriteOutcome.Conflict,
                "2" => WriteOutcome.Transient,
                _ => WriteOutcome.Conflict
            })).ToList());
        }, (conflicts, _) => Task.FromResult<IReadOnlyList<(Item?, WriteItemResult?)>>(conflicts.Select(c => ((Item?)(c with { Attempt = c.Attempt + 1 }), (WriteItemResult?)null)).ToList()), CancellationToken.None);

        // Assert
        Assert.Equal(3, rounds);
        Assert.Equal([WriteOutcome.Succeeded, WriteOutcome.Succeeded, WriteOutcome.Transient, WriteOutcome.Conflict], run.Items.Select(i => i.Result.Outcome));
        Assert.Equal(1, run.Items[1].Item.Attempt);
        Assert.Equal(2, run.Items[3].Item.Attempt);
        var exception = WriteExceptionMapper.CreateException(run, "testing", MissingDocumentPolicy.Skip);
        Assert.IsType<VersionConflictDocumentException>(exception);
    }

    [Fact]
    public async Task RunAsync_WithRetryZero_DoesNotRetry()
    {
        // Act
        int rounds = 0;
        var run = await CreateRunner().RunAsync<Item>("Test", [new Item(new DocumentKey("i", "1"))], 0, (pending, _) =>
        {
            rounds++;
            return Task.FromResult<IReadOnlyList<WriteItemResult>>([new WriteItemResult(WriteOutcome.Transient)]);
        }, null, CancellationToken.None);

        // Assert
        Assert.Equal(1, rounds);
        Assert.Equal(WriteOutcome.Transient, Assert.Single(run.Items).Result.Outcome);
    }

    [Fact]
    public async Task RunAsync_WhenCancelledBetweenRounds_EndsPendingItemsAsCancelled()
    {
        // Arrange
        using var cancellation = new CancellationTokenSource();
        var items = new[] { new Item(new DocumentKey("i", "ok")), new Item(new DocumentKey("i", "retry")) };

        // Act
        var run = await CreateRunner().RunAsync<Item>("Test", items, 5, (pending, _) =>
        {
            cancellation.Cancel();
            return Task.FromResult<IReadOnlyList<WriteItemResult>>(pending.Select(p => new WriteItemResult(p.Key.Id is "ok" ? WriteOutcome.Succeeded : WriteOutcome.Transient)).ToList());
        }, null, cancellation.Token);

        // Assert
        Assert.Equal([WriteOutcome.Succeeded, WriteOutcome.Cancelled], run.Items.Select(i => i.Result.Outcome));
        var exception = WriteExceptionMapper.CreateException(run, "testing", MissingDocumentPolicy.Skip);
        Assert.IsType<OperationCanceledException>(exception);
        Assert.Contains("1 documents not confirmed", exception.Message);
    }

    [Fact]
    public async Task RunAsync_WhenConflictHandlerThrows_EndsConflictsAsFatalAndKeepsSuccesses()
    {
        // Arrange
        var items = new[] { new Item(new DocumentKey("i", "ok")), new Item(new DocumentKey("i", "conflict")) };

        // Act
        var run = await CreateRunner().RunAsync<Item>("Test", items, 5,
            (pending, _) => Task.FromResult<IReadOnlyList<WriteItemResult>>(pending.Select(p => new WriteItemResult(p.Key.Id is "ok" ? WriteOutcome.Succeeded : WriteOutcome.Conflict)).ToList()),
            (_, _) => throw new DocumentException("re-read failed"), CancellationToken.None);

        // Assert
        Assert.Equal([WriteOutcome.Succeeded, WriteOutcome.Fatal], run.Items.Select(i => i.Result.Outcome));
        Assert.Equal("re-read failed", run.Items[1].Result.Reason);
    }

    [Fact]
    public async Task RunAsync_WhenSendReturnsWrongResultCount_EndsPendingAsFatalAndKeepsEarlierSuccesses()
    {
        // Arrange
        var items = new[] { new Item(new DocumentKey("i", "ok")), new Item(new DocumentKey("i", "retry")) };
        int rounds = 0;

        // Act
        var run = await CreateRunner().RunAsync<Item>("Test", items, 5, (pending, _) =>
        {
            rounds++;
            return Task.FromResult<IReadOnlyList<WriteItemResult>>(rounds is 1
                ? pending.Select(p => new WriteItemResult(p.Key.Id is "ok" ? WriteOutcome.Succeeded : WriteOutcome.Transient)).ToList()
                : []);
        }, null, CancellationToken.None);

        // Assert
        Assert.Equal([WriteOutcome.Succeeded, WriteOutcome.Fatal], run.Items.Select(i => i.Result.Outcome));
        Assert.Contains("returned 0 results for 1 items", run.Items[1].Result.Reason);
        Assert.IsType<DocumentException>(WriteExceptionMapper.CreateException(run, "testing", MissingDocumentPolicy.Skip));
    }

    [Fact]
    public async Task RunAsync_WhenConflictHandlerReturnsWrongCount_EndsConflictsAsFatalAndKeepsSuccesses()
    {
        // Arrange
        var items = new[] { new Item(new DocumentKey("i", "ok")), new Item(new DocumentKey("i", "conflict")) };

        // Act
        var run = await CreateRunner().RunAsync<Item>("Test", items, 5,
            (pending, _) => Task.FromResult<IReadOnlyList<WriteItemResult>>(pending.Select(p => new WriteItemResult(p.Key.Id is "ok" ? WriteOutcome.Succeeded : WriteOutcome.Conflict)).ToList()),
            (_, _) => Task.FromResult<IReadOnlyList<(Item?, WriteItemResult?)>>([]), CancellationToken.None);

        // Assert
        Assert.Equal([WriteOutcome.Succeeded, WriteOutcome.Fatal], run.Items.Select(i => i.Result.Outcome));
        Assert.Contains("prepared 0 retries for 1 conflicts", run.Items[1].Result.Reason);
    }

    [Fact]
    public void AccumulateAndMerge_CarryDroppedCountsAndKeepOnlyItemsNeedingReport()
    {
        // Arrange
        static WriteRunResult<Item> Run(params WriteOutcome[] outcomes) => new(outcomes.Select((o, i) => new WriteItemOutcome<Item>(new Item(new DocumentKey("i", i.ToString())), new WriteItemResult(o, 404))).ToList(), 1);
        var decided = new[] { new WriteItemOutcome<Item>(new Item(new DocumentKey("i", "noop")), new WriteItemResult(WriteOutcome.Noop)) };

        // Act
        var total = WriteRunResult<Item>.Accumulate(null, Run(WriteOutcome.Succeeded, WriteOutcome.Noop, WriteOutcome.Missing, WriteOutcome.Fatal));
        total = WriteRunResult<Item>.Accumulate(total, Run(WriteOutcome.Succeeded, WriteOutcome.Missing));
        var merged = WriteRunResult<Item>.Merge(decided, total);

        // Assert
        Assert.Equal(3, total.OtherSucceeded);
        Assert.Equal(2, total.OtherMissing);
        Assert.Equal([WriteOutcome.Fatal], total.Items.Select(i => i.Result.Outcome));
        Assert.Equal(3, merged.OtherSucceeded);
        Assert.Equal(2, merged.OtherMissing);
        Assert.Equal(2, merged.Items.Count);
        Assert.Contains("(1 failed, 4 succeeded)", WriteExceptionMapper.CreateException(merged, "testing", MissingDocumentPolicy.Skip)!.Message);
    }

    [Fact]
    public void CreateException_UsesOnePrecedence()
    {
        static WriteRunResult<Item> Run(params WriteOutcome[] outcomes) => new(outcomes.Select((o, i) => new WriteItemOutcome<Item>(new Item(new DocumentKey("i", i.ToString())), new WriteItemResult(o, 404))).ToList(), 1);

        Assert.IsType<OperationCanceledException>(WriteExceptionMapper.CreateException(Run(WriteOutcome.Fatal, WriteOutcome.Conflict, WriteOutcome.Cancelled), "x", MissingDocumentPolicy.Skip));
        Assert.IsType<DocumentVersioningNotSupportedException>(WriteExceptionMapper.CreateException(Run(WriteOutcome.Fatal, WriteOutcome.Conflict, WriteOutcome.VersioningNotSupported), "x", MissingDocumentPolicy.Skip));
        Assert.IsType<VersionConflictDocumentException>(WriteExceptionMapper.CreateException(Run(WriteOutcome.Fatal, WriteOutcome.Conflict, WriteOutcome.Missing), "x", MissingDocumentPolicy.NotFound));
        Assert.IsType<DuplicateDocumentException>(WriteExceptionMapper.CreateException(Run(WriteOutcome.Conflict), "x", MissingDocumentPolicy.Skip, _ => true));
        Assert.IsType<DocumentNotFoundException>(WriteExceptionMapper.CreateException(Run(WriteOutcome.Fatal, WriteOutcome.Missing), "x", MissingDocumentPolicy.NotFound));
        Assert.IsType<VersionConflictDocumentException>(WriteExceptionMapper.CreateException(Run(WriteOutcome.Missing), "x", MissingDocumentPolicy.Conflict));
        Assert.IsType<DocumentException>(WriteExceptionMapper.CreateException(Run(WriteOutcome.Succeeded, WriteOutcome.Transient), "x", MissingDocumentPolicy.Skip));
        Assert.Null(WriteExceptionMapper.CreateException(Run(WriteOutcome.Succeeded, WriteOutcome.Noop, WriteOutcome.Missing), "x", MissingDocumentPolicy.Success));
    }

}
