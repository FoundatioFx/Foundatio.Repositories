using System;
using System.Text.RegularExpressions;
using Foundatio.Repositories.Exceptions;

namespace Foundatio.Repositories.Elasticsearch;

/// <summary>
/// A write failed with a transient error (HTTP 429, 502, 503, 504 or a transport failure) and may succeed if retried.
/// </summary>
internal sealed class TransientDocumentException : DocumentException
{
    public TransientDocumentException(string message, int? statusCode, Exception? innerException) : base(message, innerException)
    {
        StatusCode = statusCode;
    }

    public int? StatusCode { get; }
}

/// <summary>
/// Classifies Elasticsearch write errors for retry, conflict and configuration handling.
/// </summary>
internal static partial class ElasticWriteErrors
{
    public static bool IsTransientStatus(int? status) => status is 429 or 502 or 503 or 504;

    public static bool IsVersioningNotSupported(string? errorType)
    {
        return errorType is "o_c_c_not_supported_exception" or "occ_not_supported_exception" or "update_not_supported_exception";
    }

    public static string VersioningNotSupportedMessage(string index)
    {
        return $"Index {index} has sequence numbers disabled (index.disable_sequence_numbers), so optimistic concurrency checks and the Update API are not available. Store versioned documents in an index with sequence numbers enabled, or use SkipVersionCheck with JsonPatch or ActionPatch.";
    }

    /// <summary>
    /// <c>true</c> when a 409 reason says the document no longer exists (for example it was deleted after it was read).
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
