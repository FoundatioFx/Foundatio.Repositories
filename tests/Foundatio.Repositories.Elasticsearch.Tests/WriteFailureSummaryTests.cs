using System;
using System.Linq;
using System.Runtime.CompilerServices;
using Foundatio.Repositories.Exceptions;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class WriteFailureSummaryTests
{
    private sealed record DocumentItem(DocumentKey Key, byte[] Document) : IWriteItem
    {
        public ElasticDocumentVersion Version => new(1, 0);
    }

    private sealed record Item(DocumentKey Key, bool IsCreate = false) : IWriteItem
    {
        public ElasticDocumentVersion Version => new(1, 0);
    }

    [Fact]
    public void Add_WhenBatchesFinish_DoesNotRetainDocumentPayloads()
    {
        var summary = AccumulateFailedBatches(out var documents);

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        bool retainsDocuments = documents.Any(reference => reference.TryGetTarget(out _));
        GC.KeepAlive(summary);

        Assert.False(retainsDocuments);
    }

    [Fact]
    public void Add_WithManyFailures_KeepsBoundedSamplesAndExactCounts()
    {
        var summary = new WriteFailureSummary(MissingDocumentPolicy.Conflict);
        for (int batch = 0; batch < 100; batch++)
        {
            summary.Add(Run(batch + 1,
                Outcome($"fatal-{batch}", WriteOutcome.Fatal),
                Outcome($"conflict-{batch}", WriteOutcome.Conflict),
                Outcome($"cancelled-{batch}", WriteOutcome.Cancelled),
                Outcome($"missing-{batch}", WriteOutcome.Missing, errorType: "index_not_found_exception"),
                Outcome($"unsupported-{batch}", WriteOutcome.VersioningNotSupported)));
        }

        Assert.Equal(500, summary.FailedCount);
        Assert.Equal(0, summary.SucceededCount);
        Assert.Equal(100, summary.CancelledCount);
        Assert.Equal(200, summary.ConflictCount);
        Assert.Equal(100, summary.MissingIndexCount);
        Assert.Equal(100, summary.MissingCount);
        Assert.Equal(100, summary.Rounds);
        Assert.Equal(10, summary.Failures.Count);
        Assert.Equal(10, summary.Cancelled.Count);
        Assert.Equal(10, summary.Conflicts.Count);
        Assert.Equal(10, summary.MissingIndexes.Count);
        Assert.NotNull(summary.VersioningNotSupported);
        Assert.NotNull(summary.FirstMissing);
    }

    [Fact]
    public void Add_WithSuccessfulAndMissingBatches_AccumulatesCountsWithoutFailureSamples()
    {
        var summary = new WriteFailureSummary(MissingDocumentPolicy.Skip);
        summary.Add(Run(1, Outcome("ok-first", WriteOutcome.Succeeded), Outcome("noop", WriteOutcome.Noop), Outcome("gone-first", WriteOutcome.Missing)));
        summary.Add(Run(2, Outcome("ok-second", WriteOutcome.Succeeded), Outcome("gone-second", WriteOutcome.Missing)));

        Assert.Equal(3, summary.SucceededCount);
        Assert.Equal(2, summary.MissingCount);
        Assert.Equal(0, summary.FailedCount);
        Assert.Equal(2, summary.Rounds);
        Assert.Empty(summary.Failures);
        Assert.Empty(summary.Cancelled);
        Assert.Empty(summary.Conflicts);
        Assert.Empty(summary.MissingIndexes);
        Assert.Null(WriteExceptionMapper.CreateException(summary, "patching"));
    }

    [Theory]
    [InlineData("cancelled", typeof(OperationCanceledException))]
    [InlineData("unsupported", typeof(DocumentVersioningNotSupportedException))]
    [InlineData("missing-index", typeof(DocumentException))]
    [InlineData("conflict", typeof(VersionConflictDocumentException))]
    [InlineData("missing-document", typeof(DocumentNotFoundException))]
    public void CreateException_WithLateHigherPriorityFailure_PreservesPrecedence(string kind, Type expectedType)
    {
        var missingPolicy = kind is "missing-document" ? MissingDocumentPolicy.NotFound : MissingDocumentPolicy.Conflict;
        var summary = new WriteFailureSummary(missingPolicy);
        summary.Add(Run(2, Enumerable.Range(0, 20).Select(index => Outcome($"fatal-{index}", WriteOutcome.Fatal)).ToArray()));
        var outcome = kind switch
        {
            "cancelled" => WriteOutcome.Cancelled,
            "unsupported" => WriteOutcome.VersioningNotSupported,
            "missing-index" or "missing-document" => WriteOutcome.Missing,
            _ => WriteOutcome.Conflict
        };
        summary.Add(Run(3, Outcome("late-id", outcome, errorType: kind is "missing-index" ? "index_not_found_exception" : null, index: "late-index")));

        var exception = WriteExceptionMapper.CreateException(summary, "patching");

        Assert.IsType(expectedType, exception);
        Assert.Contains("late-", exception!.Message);
        if (kind is "missing-index")
            Assert.Contains("index late-index does not exist", exception.Message);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void CreateException_WithMoreConflictsThanSamples_ChecksEveryCreateFlag(bool lastIsCreate)
    {
        var items = Enumerable.Range(0, 12).Select(index => Outcome(index.ToString(), WriteOutcome.Conflict, isCreate: index < 11 || lastIsCreate)).ToArray();
        var summary = new WriteFailureSummary(MissingDocumentPolicy.Conflict);
        summary.Add(Run(4, items), item => item.IsCreate);

        var exception = WriteExceptionMapper.CreateException(summary, "saving");

        if (lastIsCreate)
            Assert.IsType<DuplicateDocumentException>(exception);
        else
            Assert.IsType<VersionConflictDocumentException>(exception);
        Assert.Contains("(12 failed, 0 succeeded)", exception!.Message);
        Assert.Contains("Ids: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, ...", exception.Message);
    }

    [Fact]
    public void CreateException_WithLateErrorDetails_PreservesFirstReasonAndException()
    {
        var summary = new WriteFailureSummary(MissingDocumentPolicy.Skip);
        summary.Add(Run(2, Enumerable.Range(0, 20).Select(index => Outcome($"fatal-{index}", WriteOutcome.Fatal)).ToArray()));
        var firstException = new InvalidOperationException("first exception");
        summary.Add(Run(5,
            Outcome("late-first", WriteOutcome.Fatal, reason: "first reason", exception: firstException),
            Outcome("late-second", WriteOutcome.Transient, reason: "second reason", exception: new Exception("second exception")),
            Outcome("ok", WriteOutcome.Succeeded),
            Outcome("noop", WriteOutcome.Noop),
            Outcome("gone", WriteOutcome.Missing)));

        var exception = Assert.IsType<DocumentException>(WriteExceptionMapper.CreateException(summary, "patching"));

        Assert.Equal(5, summary.Rounds);
        Assert.Equal(22, summary.FailedCount);
        Assert.Equal(2, summary.SucceededCount);
        Assert.Equal(1, summary.MissingCount);
        Assert.Same(firstException, exception.InnerException);
        Assert.Contains("22 failures (22 failed, 2 succeeded)", exception.Message);
        Assert.Contains("First error: first reason", exception.Message);
        Assert.Contains("Ids: fatal-0, fatal-1, fatal-2, fatal-3, fatal-4, fatal-5, fatal-6, fatal-7, fatal-8, fatal-9, ...", exception.Message);
        Assert.DoesNotContain("late-", exception.Message);
    }

    [Fact]
    public void CreateException_WithSuccessfulRun_DoesNotAllocate()
    {
        var run = Run(1, Outcome("ok", WriteOutcome.Succeeded), Outcome("noop", WriteOutcome.Noop), Outcome("gone", WriteOutcome.Missing));
        _ = WriteExceptionMapper.CreateException(run, "patching", MissingDocumentPolicy.Skip);

        long before = GC.GetAllocatedBytesForCurrentThread();
        bool hasException = false;
        for (int iteration = 0; iteration < 100; iteration++)
            hasException |= WriteExceptionMapper.CreateException(run, "patching", MissingDocumentPolicy.Skip) is not null;
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.False(hasException);
        Assert.Equal(0, allocated);
    }

    private static WriteRunResult<Item> Run(int rounds, params WriteItemOutcome<Item>[] outcomes) => new(outcomes, rounds);

    private static WriteItemOutcome<Item> Outcome(string id, WriteOutcome outcome, string? reason = null, Exception? exception = null, string? errorType = null, string index = "index", bool isCreate = false)
    {
        return new WriteItemOutcome<Item>(new Item(new DocumentKey(index, id), isCreate), new WriteItemResult(outcome, ErrorType: errorType, Reason: reason, Exception: exception));
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WriteFailureSummary AccumulateFailedBatches(out WeakReference<byte[]>[] documents)
    {
        const int batches = 8;
        const int batchSize = 16;
        documents = new WeakReference<byte[]>[batches * batchSize];
        var summary = new WriteFailureSummary(MissingDocumentPolicy.Skip);
        for (int batch = 0; batch < batches; batch++)
        {
            var items = new WriteItemOutcome<DocumentItem>[batchSize];
            for (int index = 0; index < batchSize; index++)
            {
                int slot = batch * batchSize + index;
                byte[] document = new byte[4096];
                documents[slot] = new WeakReference<byte[]>(document);
                var item = new DocumentItem(new DocumentKey("index", slot.ToString()), document);
                items[index] = new WriteItemOutcome<DocumentItem>(item, new WriteItemResult(WriteOutcome.Fatal, 400, Reason: "mapping failure"));
            }

            summary.Add(new WriteRunResult<DocumentItem>(items, 1));
        }

        return summary;
    }
}
