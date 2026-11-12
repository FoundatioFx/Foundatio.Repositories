using System;
using System.Net.Http;
using Elastic.Transport;

namespace Foundatio.Repositories.Elasticsearch;

/// <summary>
/// Identifies a document in a write: its concrete index and id. Ids are only unique per index, so every per-item
/// outcome is keyed by both.
/// </summary>
internal readonly record struct DocumentKey(string Index, string Id)
{
    public override string ToString() => $"{Index}/{Id}";
}

/// <summary>
/// The terminal or intermediate outcome of one document in a write round.
/// </summary>
/// <remarks>
/// <see cref="Conflict"/> and <see cref="Transient"/> are the only outcomes a round runner may retry. Every other value is
/// terminal. When a runner stops (budget used, cancellation or a non-retryable request failure) every item still pending
/// is assigned the outcome that stopped it, so no item is ever dropped.
/// </remarks>
internal enum WriteOutcome
{
    /// <summary>The write was applied.</summary>
    Succeeded,

    /// <summary>The write was accepted but changed nothing (Update API <c>noop</c>, or an action patch that made no change).</summary>
    Noop,

    /// <summary>HTTP 409: the document changed since the version the write was based on.</summary>
    Conflict,

    /// <summary>The document does not exist (HTTP 404, or a 409 whose reason says no document was found).</summary>
    Missing,

    /// <summary>HTTP 429 or 503, or a connection failure where the request was never sent. Safe to retry.</summary>
    Transient,

    /// <summary>The index has sequence numbers disabled, so the version check or Update API is not available.</summary>
    VersioningNotSupported,

    /// <summary>Any other failure, including 502, 504, timeouts and failures with an unknown status after the request was sent.</summary>
    Fatal,

    /// <summary>The operation was cancelled before this document reached another terminal outcome.</summary>
    Cancelled
}

/// <summary>
/// Classifies Elasticsearch write failures into <see cref="WriteOutcome"/> values. This is the single place that decides
/// what is retried.
/// </summary>
internal static class WriteOutcomeClassifier
{
    /// <summary>
    /// Classifies a failed bulk item or single-document response.
    /// </summary>
    public static WriteOutcome Classify(int? status, string? errorType, string? reason)
    {
        if (ElasticWriteErrors.IsVersioningNotSupported(errorType))
            return WriteOutcome.VersioningNotSupported;

        return status switch
        {
            404 => WriteOutcome.Missing,
            409 when ElasticWriteErrors.IsMissingDocumentConflict(reason) => WriteOutcome.Missing,
            409 => WriteOutcome.Conflict,
            _ when ElasticWriteErrors.IsTransientStatus(status) => WriteOutcome.Transient,
            _ => WriteOutcome.Fatal
        };
    }

    /// <summary>
    /// Classifies a request that failed as a whole (no per-item results). Only HTTP 429/503 and connection failures where
    /// the request was definitely not sent are <see cref="WriteOutcome.Transient"/>. HTTP 502, 504, timeouts and failures
    /// with no status after the request was sent are <see cref="WriteOutcome.Fatal"/>: the write may have been applied, so
    /// re-sending it could apply it twice.
    /// </summary>
    public static WriteOutcome ClassifyRequestFailure(int? status, Exception? exception)
    {
        if (status is not null)
            return ElasticWriteErrors.IsTransientStatus(status) ? WriteOutcome.Transient : WriteOutcome.Fatal;

        return IsNotSent(exception) ? WriteOutcome.Transient : WriteOutcome.Fatal;
    }

    /// <summary>
    /// <c>true</c> when the exception chain shows the request never reached Elasticsearch: the connection could not be
    /// established (<see cref="HttpRequestError.ConnectionError"/>, <see cref="HttpRequestError.NameResolutionError"/>,
    /// <see cref="HttpRequestError.SecureConnectionError"/>) or the transport attempted no node
    /// (<see cref="PipelineFailure.NoNodesAttempted"/>).
    /// </summary>
    public static bool IsNotSent(Exception? exception)
    {
        for (var current = exception; current is not null; current = current.InnerException)
        {
            switch (current)
            {
                case HttpRequestException { HttpRequestError: HttpRequestError.ConnectionError or HttpRequestError.NameResolutionError or HttpRequestError.SecureConnectionError }:
                case TransportException { FailureReason: PipelineFailure.NoNodesAttempted }:
                    return true;
            }
        }

        return false;
    }
}
