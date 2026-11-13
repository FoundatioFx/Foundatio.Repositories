using System;
using System.Text.RegularExpressions;

namespace Foundatio.Repositories.Elasticsearch;

/// <summary>
/// Classifies Elasticsearch write errors for retry, conflict and configuration handling.
/// </summary>
internal static partial class ElasticWriteErrors
{
    /// <summary>
    /// <c>true</c> for HTTP 429 and 503, where Elasticsearch rejected the request before applying it. HTTP 502 and 504
    /// come from a proxy or gateway that may already have forwarded the write, so they are not retried.
    /// </summary>
    public static bool IsTransientStatus(int? status) => status is 429 or 503;

    public static bool IsVersioningNotSupported(string? errorType)
    {
        return errorType is "o_c_c_not_supported_exception" or "occ_not_supported_exception" or "update_not_supported_exception";
    }

    public static bool IsIndexNotFound(string? errorType) => errorType is "index_not_found_exception";

    public static string VersioningNotSupportedMessage(string index)
    {
        return $"Index {index} has sequence numbers disabled (index.disable_sequence_numbers), so optimistic concurrency checks and the Update API are not available. Store versioned documents in an index with sequence numbers enabled, or use SkipVersionCheck with JsonPatch or ActionPatch.";
    }

    /// <summary>
    /// <c>true</c> when a 409 reason says the document no longer exists (a version-checked write or delete found no
    /// document, for example because it was deleted after it was read). Elasticsearch reports this as
    /// <c>version_conflict_engine_exception</c> with a reason ending in <c>but no document was found</c>; the contract is
    /// pinned by an integration test against a real cluster.
    /// </summary>
    public static bool IsMissingDocumentConflict(string? reason)
    {
        return reason is not null && reason.Contains("no document was found", StringComparison.Ordinal);
    }

    /// <summary>
    /// Parses the expected and current versions from a version conflict reason such as
    /// <c>required seqNo [5], primary term [1]. current document has seqNo [6] and primary term [1]</c>.
    /// </summary>
    public static (string? Expected, string? Current) ParseConflictVersions(string? reason)
    {
        if (String.IsNullOrEmpty(reason))
            return (null, null);

        var match = ConflictRegex().Match(reason);
        if (!match.Success)
            return (null, null);

        string expected = $"{match.Groups["expectedTerm"].Value}:{match.Groups["expectedSeqNo"].Value}";
        string? current = match.Groups["currentSeqNo"].Success
            ? $"{match.Groups["currentTerm"].Value}:{match.Groups["currentSeqNo"].Value}"
            : null;

        return (expected, current);
    }

    [GeneratedRegex(@"required seqNo \[(?<expectedSeqNo>-?\d+)\], primary term \[(?<expectedTerm>-?\d+)\](?:\. current document has seqNo \[(?<currentSeqNo>-?\d+)\] and primary term \[(?<currentTerm>-?\d+)\])?", RegexOptions.CultureInvariant)]
    private static partial Regex ConflictRegex();
}
