using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.Utility;
using Microsoft.Extensions.Logging;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;

namespace Foundatio.Repositories.Elasticsearch;

/// <summary>An item a <see cref="WriteRoundRunner"/> writes: a document key and the version the write is based on.</summary>
internal interface IWriteItem
{
    DocumentKey Key { get; }

    /// <summary>The version sent as <c>if_seq_no</c>/<c>if_primary_term</c>, or empty for an unchecked write.</summary>
    ElasticDocumentVersion Version { get; }
}

/// <summary>The outcome of one item in one round, or its terminal outcome once the runner finishes.</summary>
internal sealed record WriteItemResult(WriteOutcome Outcome, int? Status = null, string? ErrorType = null, string? Reason = null, Exception? Exception = null, ElasticDocumentVersion Version = default)
{
    public static readonly WriteItemResult Cancelled = new(WriteOutcome.Cancelled);
}

/// <summary>The terminal result of one item: the last version of the item that was sent, and its outcome.</summary>
internal readonly record struct WriteItemOutcome<TItem>(TItem Item, WriteItemResult Result) where TItem : IWriteItem;

/// <summary>
/// Sends one round. Must return exactly one result per pending item, in the same order.
/// </summary>
internal delegate Task<IReadOnlyList<WriteItemResult>> SendWriteRound<TItem>(IReadOnlyList<TItem> pending, CancellationToken cancellationToken) where TItem : IWriteItem;

/// <summary>
/// Prepares conflicting items for the next round (for example re-reads and re-patches them). Returns, per conflicting item
/// and in the same order, either the item to send next (<c>Retry</c>) or its terminal result (<c>Terminal</c>).
/// </summary>
internal delegate Task<IReadOnlyList<(TItem? Retry, WriteItemResult? Terminal)>> PrepareConflictRetry<TItem>(IReadOnlyList<TItem> conflicts, CancellationToken cancellationToken) where TItem : IWriteItem;

/// <summary>
/// Runs a write as bounded rounds over a set of items and guarantees every item ends with a terminal
/// <see cref="WriteOutcome"/>. This is the only retry loop used by repository writes.
/// </summary>
/// <remarks>
/// <para><b>States.</b> Each item is <i>pending</i> until it reaches a terminal outcome. A round sends every pending item
/// once and classifies each result.</para>
/// <para><b>Transitions.</b> <see cref="WriteOutcome.Transient"/> items are sent again unchanged in the next round.
/// <see cref="WriteOutcome.Conflict"/> items are passed to the conflict handler, which re-reads them (fresh version) or
/// ends them; with no handler a conflict is terminal. Every other outcome is terminal.</para>
/// <para><b>Budget.</b> <c>Retry(n)</c> allows <c>n</c> retries, so at most <c>n + 1</c> rounds. Rounds are separated by
/// exponential backoff (50 ms doubling, capped at 2 s) with ±25% jitter. After the last round, items still transient or
/// conflicting keep that outcome as their terminal outcome.</para>
/// <para><b>Cancellation and failures.</b> When the token is cancelled, every pending item ends as
/// <see cref="WriteOutcome.Cancelled"/>. If sending or the conflict handler throws, the affected items end as
/// <see cref="WriteOutcome.Fatal"/> with the exception. Items that already succeeded keep their outcome, so callers can
/// invalidate and notify them before reporting the failures.</para>
/// </remarks>
internal sealed class WriteRoundRunner
{
    private static readonly TimeSpan _baseDelay = TimeSpan.FromMilliseconds(50);
    private static readonly TimeSpan _maxDelay = TimeSpan.FromSeconds(2);

    private readonly ILogger _logger;
    private readonly TimeProvider _timeProvider;

    public WriteRoundRunner(ILogger logger, TimeProvider timeProvider)
    {
        _logger = logger;
        _timeProvider = timeProvider;
    }

    /// <summary>
    /// Writes <paramref name="items"/> in rounds until each has a terminal outcome. <paramref name="retryBudget"/> is
    /// <c>Retry(n)</c>; pass <c>null</c> for <paramref name="prepareConflictRetry"/> to make conflicts terminal.
    /// </summary>
    public async Task<WriteRunResult<TItem>> RunAsync<TItem>(string operation, IReadOnlyList<TItem> items, int retryBudget, SendWriteRound<TItem> send, PrepareConflictRetry<TItem>? prepareConflictRetry, CancellationToken cancellationToken)
        where TItem : IWriteItem
    {
        var current = items.ToArray();
        var results = new WriteItemResult?[current.Length];
        var pending = Enumerable.Range(0, current.Length).ToList();
        int round = 0;

        while (pending.Count > 0)
        {
            round++;
            if (cancellationToken.IsCancellationRequested)
            {
                Complete(pending, results, WriteItemResult.Cancelled);
                break;
            }

            IReadOnlyList<WriteItemResult> sent;
            try
            {
                sent = await send(pending.Select(slot => current[slot]).ToList(), cancellationToken).AnyContext();
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                Complete(pending, results, WriteItemResult.Cancelled);
                break;
            }
            catch (Exception ex)
            {
                Complete(pending, results, new WriteItemResult(WriteOutcome.Fatal, Reason: ex.Message, Exception: ex));
                break;
            }

            if (sent.Count != pending.Count)
            {
                Complete(pending, results, new WriteItemResult(WriteOutcome.Fatal, Reason: $"{operation} returned {sent.Count} results for {pending.Count} items."));
                break;
            }

            if (cancellationToken.IsCancellationRequested)
                sent = sent.Select(r => r.Outcome is WriteOutcome.Fatal or WriteOutcome.Transient && r.Status is null ? WriteItemResult.Cancelled : r).ToList();

            bool canRetry = round <= retryBudget;
            var transient = new List<int>();
            var conflicts = new List<int>();
            for (int index = 0; index < pending.Count; index++)
            {
                int slot = pending[index];
                var result = sent[index];
                if (result.Outcome is WriteOutcome.Conflict)
                    LogConflict(operation, current[slot], result, round, retryBudget, canRetry && prepareConflictRetry is not null);

                if (canRetry && result.Outcome is WriteOutcome.Transient)
                    transient.Add(slot);
                else if (canRetry && prepareConflictRetry is not null && result.Outcome is WriteOutcome.Conflict)
                    conflicts.Add(slot);
                else
                    results[slot] = result;
            }

            if (transient.Count is 0 && conflicts.Count is 0)
                break;

            _logger.LogInformation("{Operation} round {Round} of {Rounds}: {Transient} transient failures and {Conflicts} conflicts will be retried",
                operation, round, retryBudget + 1, transient.Count, conflicts.Count);

            await _timeProvider.SafeDelay(GetDelay(round), cancellationToken).AnyContext();
            pending = [.. transient];
            if (cancellationToken.IsCancellationRequested)
            {
                Complete(pending.Concat(conflicts), results, WriteItemResult.Cancelled);
                pending.Clear();
                break;
            }

            if (conflicts.Count > 0)
                await PrepareConflictsAsync(operation, conflicts, current, results, pending, prepareConflictRetry!, cancellationToken).AnyContext();
        }

        var outcomes = new WriteItemOutcome<TItem>[current.Length];
        for (int slot = 0; slot < current.Length; slot++)
            outcomes[slot] = new WriteItemOutcome<TItem>(current[slot], results[slot] ?? throw new InvalidOperationException($"{operation} left item {current[slot].Key} without an outcome."));

        var run = new WriteRunResult<TItem>(outcomes, round);
        if (run.Count(WriteOutcome.Conflict) + run.Count(WriteOutcome.Transient) > 0)
        {
            _logger.LogError("{Operation} stopped after {Rounds} rounds with {Conflicts} documents still conflicting and {Transient} still failing with transient errors",
                operation, round, run.Count(WriteOutcome.Conflict), run.Count(WriteOutcome.Transient));
        }

        return run;
    }

    private static async Task PrepareConflictsAsync<TItem>(string operation, List<int> conflicts, TItem[] current, WriteItemResult?[] results, List<int> pending, PrepareConflictRetry<TItem> prepare, CancellationToken cancellationToken)
        where TItem : IWriteItem
    {
        IReadOnlyList<(TItem? Retry, WriteItemResult? Terminal)> prepared;
        try
        {
            prepared = await prepare(conflicts.Select(slot => current[slot]).ToList(), cancellationToken).AnyContext();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            Complete(conflicts.Concat(pending), results, WriteItemResult.Cancelled);
            pending.Clear();
            return;
        }
        catch (Exception ex)
        {
            Complete(conflicts, results, new WriteItemResult(WriteOutcome.Fatal, Reason: ex.Message, Exception: ex));
            return;
        }

        if (prepared.Count != conflicts.Count)
        {
            Complete(conflicts, results, new WriteItemResult(WriteOutcome.Fatal, Reason: $"{operation} prepared {prepared.Count} retries for {conflicts.Count} conflicts."));
            return;
        }

        for (int index = 0; index < conflicts.Count; index++)
        {
            int slot = conflicts[index];
            var (retry, terminal) = prepared[index];
            if (retry is not null)
            {
                current[slot] = retry;
                pending.Add(slot);
            }
            else
            {
                results[slot] = terminal ?? new WriteItemResult(WriteOutcome.Fatal, Reason: "No retry or outcome was produced for this document.");
            }
        }
    }

    private static void Complete(IEnumerable<int> slots, WriteItemResult?[] results, WriteItemResult result)
    {
        foreach (int slot in slots)
            results[slot] ??= result;
    }

    private void LogConflict<TItem>(string operation, TItem item, WriteItemResult result, int round, int retryBudget, bool willRetry) where TItem : IWriteItem
    {
        if (!_logger.IsEnabled(LogLevel.Warning))
            return;

        var (parsedExpected, currentVersion) = ElasticWriteErrors.ParseConflictVersions(result.Reason);
        _logger.LogWarning("Version conflict during {Operation} on {Index}/{DocumentId}: attempt {Attempt} of {Budget}, expected version {ExpectedVersion}, current version {CurrentVersion}, outcome {Outcome}",
            operation, item.Key.Index, item.Key.Id, round, retryBudget + 1, item.Version.ToVersionString() ?? parsedExpected, currentVersion, willRetry ? "Retrying" : "Failed");
    }

    internal static TimeSpan GetDelay(int round)
    {
        double milliseconds = Math.Min(_baseDelay.TotalMilliseconds * Math.Pow(2, Math.Max(0, round - 1)), _maxDelay.TotalMilliseconds);
        double jitter = milliseconds * 0.25 * ((Random.Shared.NextDouble() * 2) - 1);
        return TimeSpan.FromMilliseconds(Math.Max(0, milliseconds + jitter));
    }
}

/// <summary>The terminal outcome of every item passed to <see cref="WriteRoundRunner.RunAsync{TItem}"/>.</summary>
internal sealed class WriteRunResult<TItem> where TItem : IWriteItem
{
    public WriteRunResult(IReadOnlyList<WriteItemOutcome<TItem>> items, int rounds, int otherSucceeded = 0, int otherMissing = 0)
    {
        Items = items;
        Rounds = rounds;
        OtherSucceeded = otherSucceeded;
        OtherMissing = otherMissing;
    }

    public IReadOnlyList<WriteItemOutcome<TItem>> Items { get; }
    public int Rounds { get; }

    /// <summary>Items that succeeded in earlier batches and were dropped by <see cref="Accumulate"/> to bound memory.</summary>
    public int OtherSucceeded { get; }

    /// <summary>Items that no longer existed in earlier batches and were dropped by <see cref="Accumulate"/>; they are not failures when missing documents are skipped.</summary>
    public int OtherMissing { get; }

    /// <summary>
    /// Adds a batch to a running total, keeping only the items that still need reporting (so a long <c>PatchAllAsync</c>
    /// does not hold every patched document) and counting the rest.
    /// </summary>
    public static WriteRunResult<TItem> Accumulate(WriteRunResult<TItem>? total, WriteRunResult<TItem> batch)
    {
        var kept = (total?.Items ?? []).Concat(batch.Items.Where(i => i.Result.Outcome is not (WriteOutcome.Succeeded or WriteOutcome.Noop or WriteOutcome.Missing))).ToList();
        int droppedSucceeded = batch.Items.Count(i => i.Result.Outcome is WriteOutcome.Succeeded or WriteOutcome.Noop);
        int droppedMissing = batch.Items.Count(i => i.Result.Outcome is WriteOutcome.Missing);
        return new WriteRunResult<TItem>(kept, Math.Max(total?.Rounds ?? 0, batch.Rounds),
            (total?.OtherSucceeded ?? 0) + batch.OtherSucceeded + droppedSucceeded,
            (total?.OtherMissing ?? 0) + batch.OtherMissing + droppedMissing);
    }

    public IEnumerable<WriteItemOutcome<TItem>> With(params WriteOutcome[] outcomes) => Items.Where(i => outcomes.Contains(i.Result.Outcome));
    public int Count(WriteOutcome outcome) => Items.Count(i => i.Result.Outcome == outcome);

    /// <summary>Items that were written (<see cref="WriteOutcome.Succeeded"/>), in input order.</summary>
    public IReadOnlyList<TItem> Succeeded => Items.Where(i => i.Result.Outcome is WriteOutcome.Succeeded).Select(i => i.Item).ToList();

    /// <summary>Combines outcomes decided before writing (for example a patch that made no change) with a run's outcomes.</summary>
    public static WriteRunResult<TItem> Merge(IReadOnlyCollection<WriteItemOutcome<TItem>> decided, WriteRunResult<TItem> run)
    {
        return decided.Count is 0 ? run : new WriteRunResult<TItem>([.. decided, .. run.Items], run.Rounds, run.OtherSucceeded, run.OtherMissing);
    }
}
