using System;
using System.Collections.Generic;
using System.Linq;
using Foundatio.Repositories.Exceptions;

namespace Foundatio.Repositories.Elasticsearch;

/// <summary>How an operation treats a document that turned out not to exist.</summary>
internal enum MissingDocumentPolicy
{
    /// <summary>Skipped and not reported (query-driven batch patches: the document was deleted while the batch ran).</summary>
    Skip,

    /// <summary>Not an error and not reported as a change (removes: the document is already gone; only its cache entry is evicted).</summary>
    Success,

    /// <summary>Reported as <see cref="DocumentNotFoundException"/> (patches by id).</summary>
    NotFound,

    /// <summary>Reported as a version conflict (version-checked saves: the document was deleted since it was read).</summary>
    Conflict
}

/// <summary>
/// Maps the terminal outcomes of a write to the one exception the caller sees. It is the only exception mapper for
/// repository writes, so every operation reports failures with the same types and precedence.
/// </summary>
/// <remarks>
/// Precedence, highest first: <see cref="WriteOutcome.Cancelled"/> → <see cref="OperationCanceledException"/>;
/// <see cref="WriteOutcome.VersioningNotSupported"/> → <see cref="DocumentVersioningNotSupportedException"/>;
/// a save to an index that doesn't exist (<c>index_not_found_exception</c>) → <see cref="DocumentException"/> naming the index;
/// <see cref="WriteOutcome.Conflict"/> → <see cref="DuplicateDocumentException"/> when every conflicting write was a
/// create, otherwise <see cref="VersionConflictDocumentException"/>; <see cref="WriteOutcome.Missing"/> (with
/// <see cref="MissingDocumentPolicy.NotFound"/>) → <see cref="DocumentNotFoundException"/>;
/// <see cref="WriteOutcome.Transient"/> and <see cref="WriteOutcome.Fatal"/> → <see cref="DocumentException"/>.
/// </remarks>
internal static class WriteExceptionMapper
{
    /// <summary>Returns the exception for the failed items, or <c>null</c> when every item succeeded (or was a no-op or skipped).</summary>
    public static Exception? CreateException<TItem>(WriteRunResult<TItem> run, string operationLabel, MissingDocumentPolicy missing, Func<TItem, bool>? isCreate = null)
        where TItem : IWriteItem
    {
        foreach (var item in run.AsSpan())
        {
            if (!IsFailure(item.Result.Outcome, missing))
                continue;

            var summary = new WriteFailureSummary(missing);
            summary.Add(run, isCreate);
            return CreateException(summary, operationLabel);
        }

        return null;
    }

    /// <summary>Maps accumulated diagnostics without retaining or reconstructing the original document items.</summary>
    public static Exception? CreateException(WriteFailureSummary failures, string operationLabel)
    {
        if (failures.FailedCount is 0)
            return null;

        string summary = $"({failures.FailedCount} failed, {failures.SucceededCount} succeeded)";

        if (failures.CancelledCount > 0)
            return new OperationCanceledException($"Error {operationLabel} documents: cancelled with {failures.CancelledCount} documents not confirmed {summary}. Ids: {FormatIds(failures.Cancelled, failures.CancelledCount)}");

        if (failures.VersioningNotSupported is { } unsupported)
            return new DocumentVersioningNotSupportedException(ElasticWriteErrors.VersioningNotSupportedMessage(unsupported.Key.Index), unsupported.Result.Exception);

        if (failures.MissingIndexCount > 0)
            return new DocumentException($"Error {operationLabel} documents: index {failures.MissingIndexes[0].Key.Index} does not exist {summary}. Ids: {FormatIds(failures.MissingIndexes, failures.MissingIndexCount)}", failures.MissingIndexes[0].Result.Exception);

        if (failures.ConflictCount > 0)
        {
            if (failures.AllConflictsAreCreates)
                return new DuplicateDocumentException($"Error {operationLabel} documents: {failures.ConflictCount} already exist {summary}. Ids: {FormatIds(failures.Conflicts, failures.ConflictCount)}", failures.Conflicts[0].Result.Exception);

            return new VersionConflictDocumentException($"Error {operationLabel} documents: {failures.ConflictCount} still conflicting after {failures.Rounds} rounds {summary}. Ids: {FormatIds(failures.Conflicts, failures.ConflictCount)}", failures.Conflicts[0].Result.Exception);
        }

        if (failures.FirstMissing is { } notFound)
            return new DocumentNotFoundException(notFound.Key.Id);

        string? reason = failures.FirstReason;
        return new DocumentException($"Error {operationLabel} documents: {failures.FailedCount} failures {summary}. Ids: {FormatIds(failures.Failures, failures.FailedCount)}{(reason is null ? "" : $". First error: {reason}")}", failures.FirstException);
    }

    public static void ThrowIfFailed<TItem>(WriteRunResult<TItem> run, string operationLabel, MissingDocumentPolicy missing, Func<TItem, bool>? isCreate = null)
        where TItem : IWriteItem
    {
        if (CreateException(run, operationLabel, missing, isCreate) is { } exception)
            throw exception;
    }

    public static void ThrowIfFailed(WriteFailureSummary failures, string operationLabel)
    {
        if (CreateException(failures, operationLabel) is { } exception)
            throw exception;
    }

    internal static bool IsFailure(WriteOutcome outcome, MissingDocumentPolicy missing)
    {
        return outcome switch
        {
            WriteOutcome.Succeeded or WriteOutcome.Noop => false,
            WriteOutcome.Missing => missing is MissingDocumentPolicy.NotFound or MissingDocumentPolicy.Conflict,
            _ => true
        };
    }

    private static string FormatIds(IReadOnlyList<WriteFailureSummary.Failure> samples, long count)
    {
        string ids = String.Join(", ", samples.Select(sample => sample.Key.Id));
        return count > samples.Count ? ids + ", ..." : ids;
    }
}
