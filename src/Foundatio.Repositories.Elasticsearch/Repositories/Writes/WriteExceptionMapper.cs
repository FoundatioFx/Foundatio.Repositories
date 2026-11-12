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

    /// <summary>Counts as done (removes: the document is already gone).</summary>
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
/// <see cref="WriteOutcome.Conflict"/> → <see cref="DuplicateDocumentException"/> when every conflicting write was a
/// create, otherwise <see cref="VersionConflictDocumentException"/>; <see cref="WriteOutcome.Missing"/> (with
/// <see cref="MissingDocumentPolicy.NotFound"/>) → <see cref="DocumentNotFoundException"/>;
/// <see cref="WriteOutcome.Transient"/> and <see cref="WriteOutcome.Fatal"/> → <see cref="DocumentException"/>.
/// </remarks>
internal static class WriteExceptionMapper
{
    private const int MaxIds = 10;

    /// <summary>Returns the exception for the failed items, or <c>null</c> when every item succeeded (or was a no-op or skipped).</summary>
    public static Exception? CreateException<TItem>(WriteRunResult<TItem> run, string operationLabel, MissingDocumentPolicy missing, Func<TItem, bool>? isCreate = null)
        where TItem : IWriteItem
    {
        var failed = run.Items.Where(i => IsFailure(i.Result.Outcome, missing)).ToList();
        if (failed.Count is 0)
            return null;

        int succeeded = run.Items.Count - failed.Count + run.OtherSucceeded;
        string summary = $"({failed.Count} failed, {succeeded} succeeded)";

        var cancelled = failed.Where(i => i.Result.Outcome is WriteOutcome.Cancelled).ToList();
        if (cancelled.Count > 0)
            return new OperationCanceledException($"Error {operationLabel} documents: cancelled with {cancelled.Count} documents not written {summary}. Ids: {FormatIds(cancelled)}");

        var unsupported = failed.FirstOrDefault(i => i.Result.Outcome is WriteOutcome.VersioningNotSupported);
        if (unsupported.Item is not null)
            return new DocumentVersioningNotSupportedException(ElasticWriteErrors.VersioningNotSupportedMessage(unsupported.Item.Key.Index), unsupported.Result.Exception);

        var conflicts = failed.Where(i => i.Result.Outcome is WriteOutcome.Conflict || (i.Result.Outcome is WriteOutcome.Missing && missing is MissingDocumentPolicy.Conflict)).ToList();
        if (conflicts.Count > 0)
        {
            if (isCreate is not null && conflicts.All(i => isCreate(i.Item)))
                return new DuplicateDocumentException($"Error {operationLabel} documents: {conflicts.Count} already exist {summary}. Ids: {FormatIds(conflicts)}", conflicts[0].Result.Exception);

            return new VersionConflictDocumentException($"Error {operationLabel} documents: {conflicts.Count} still conflicting after {run.Rounds} rounds {summary}. Ids: {FormatIds(conflicts)}", conflicts[0].Result.Exception);
        }

        var notFound = failed.FirstOrDefault(i => i.Result.Outcome is WriteOutcome.Missing);
        if (notFound.Item is not null)
            return new DocumentNotFoundException(notFound.Item.Key.Id);

        string? reason = failed.Select(i => i.Result.Reason).FirstOrDefault(r => r is not null);
        var inner = failed.Select(i => i.Result.Exception).FirstOrDefault(e => e is not null);
        return new DocumentException($"Error {operationLabel} documents: {failed.Count} failures {summary}. Ids: {FormatIds(failed)}{(reason is null ? "" : $". First error: {reason}")}", inner);
    }

    public static void ThrowIfFailed<TItem>(WriteRunResult<TItem> run, string operationLabel, MissingDocumentPolicy missing, Func<TItem, bool>? isCreate = null)
        where TItem : IWriteItem
    {
        if (CreateException(run, operationLabel, missing, isCreate) is { } exception)
            throw exception;
    }

    private static bool IsFailure(WriteOutcome outcome, MissingDocumentPolicy missing)
    {
        return outcome switch
        {
            WriteOutcome.Succeeded or WriteOutcome.Noop => false,
            WriteOutcome.Missing => missing is MissingDocumentPolicy.NotFound or MissingDocumentPolicy.Conflict,
            _ => true
        };
    }

    internal static string FormatIds<TItem>(IReadOnlyCollection<WriteItemOutcome<TItem>> items) where TItem : IWriteItem
    {
        var ids = items.Take(MaxIds).Select(i => i.Item.Key.Id);
        return items.Count > MaxIds ? String.Join(", ", ids) + ", ..." : String.Join(", ", ids);
    }
}
