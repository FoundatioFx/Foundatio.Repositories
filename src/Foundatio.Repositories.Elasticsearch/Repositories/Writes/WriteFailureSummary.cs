using System;
using System.Collections.Generic;

namespace Foundatio.Repositories.Elasticsearch;

/// <summary>
/// Keeps exact outcome counts and bounded failure details after a write batch has been processed, without retaining
/// its document items. The exception mapper uses the same summary for individual writes and long-running query patches.
/// </summary>
internal sealed class WriteFailureSummary(MissingDocumentPolicy missingPolicy)
{
    private const int MaxSamples = 10;
    private List<Failure>? _failures;
    private List<Failure>? _cancelled;
    private List<Failure>? _conflicts;
    private List<Failure>? _missingIndexes;

    internal readonly record struct Failure(DocumentKey Key, WriteItemResult Result);

    public long FailedCount { get; private set; }
    public long SucceededCount { get; private set; }
    public long MissingCount { get; private set; }
    public long CancelledCount { get; private set; }
    public long ConflictCount { get; private set; }
    public long MissingIndexCount { get; private set; }
    public int Rounds { get; private set; }
    public bool AllConflictsAreCreates { get; private set; } = true;
    public string? FirstReason { get; private set; }
    public Exception? FirstException { get; private set; }
    public Failure? VersioningNotSupported { get; private set; }
    public Failure? FirstMissing { get; private set; }
    public IReadOnlyList<Failure> Failures => _failures ?? [];
    public IReadOnlyList<Failure> Cancelled => _cancelled ?? [];
    public IReadOnlyList<Failure> Conflicts => _conflicts ?? [];
    public IReadOnlyList<Failure> MissingIndexes => _missingIndexes ?? [];

    public void Add<TItem>(WriteRunResult<TItem> run, Func<TItem, bool>? isCreate = null) where TItem : IWriteItem
    {
        Rounds = Math.Max(Rounds, run.Rounds);
        foreach (var item in run.AsSpan())
        {
            var outcome = item.Result.Outcome;
            if (outcome is WriteOutcome.Succeeded or WriteOutcome.Noop)
            {
                SucceededCount++;
                continue;
            }

            if (outcome is WriteOutcome.Missing)
                MissingCount++;
            if (!WriteExceptionMapper.IsFailure(outcome, missingPolicy))
                continue;

            var failure = new Failure(item.Item.Key, item.Result);
            FailedCount++;
            FirstReason ??= item.Result.Reason;
            FirstException ??= item.Result.Exception;
            AddSample(ref _failures, failure);

            if (outcome is WriteOutcome.Cancelled)
            {
                CancelledCount++;
                AddSample(ref _cancelled, failure);
            }
            else if (outcome is WriteOutcome.VersioningNotSupported)
            {
                VersioningNotSupported ??= failure;
            }

            if (outcome is WriteOutcome.Missing)
            {
                FirstMissing ??= failure;
                if (missingPolicy is MissingDocumentPolicy.Conflict && ElasticWriteErrors.IsIndexNotFound(item.Result.ErrorType))
                {
                    MissingIndexCount++;
                    AddSample(ref _missingIndexes, failure);
                }
            }

            if (outcome is WriteOutcome.Conflict || (outcome is WriteOutcome.Missing && missingPolicy is MissingDocumentPolicy.Conflict))
            {
                ConflictCount++;
                AddSample(ref _conflicts, failure);
                if (AllConflictsAreCreates)
                    AllConflictsAreCreates = isCreate?.Invoke(item.Item) ?? false;
            }
        }
    }

    private static void AddSample(ref List<Failure>? samples, Failure failure)
    {
        samples ??= new List<Failure>(MaxSamples);
        if (samples.Count < MaxSamples)
            samples.Add(failure);
    }
}
