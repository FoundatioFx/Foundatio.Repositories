using System;
using System.Net.Http;
using Elastic.Transport;

namespace Foundatio.Repositories.Elasticsearch;

/// <summary>
/// The concrete index and id of a document in a write. Routing is carried by the write item; outcomes are correlated
/// by request position so documents with the same id under different indexes or routing remain distinct.
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

    /// <summary>HTTP 429, an Elasticsearch 503 rejection, or a connection failure where the request was never sent.</summary>
    Transient,

    /// <summary>The index has sequence numbers disabled, so the version check or Update API is not available.</summary>
    VersioningNotSupported,

    /// <summary>Any other failure, including gateway 502/503/504, timeouts and unknown outcomes after the request was sent.</summary>
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
    /// Classifies a request that failed as a whole (no per-item results). HTTP 429, HTTP 503 with a parsed Elasticsearch
    /// error, and connection failures where the request was definitely not sent are <see cref="WriteOutcome.Transient"/>.
    /// A gateway can return 503 after an upstream reset even when the write was applied, so 503 without an Elasticsearch
    /// error type is <see cref="WriteOutcome.Fatal"/>, as are 502, 504, timeouts and other unknown write outcomes.
    /// </summary>
    public static WriteOutcome ClassifyRequestFailure(int? status, Exception? exception, string? errorType = null)
    {
        if (status is not null)
            return status is 429 || (status is 503 && !String.IsNullOrEmpty(errorType)) ? WriteOutcome.Transient : WriteOutcome.Fatal;

        return IsNotSent(exception) ? WriteOutcome.Transient : WriteOutcome.Fatal;
    }

    /// <summary>Reads can safely retry HTTP 503 even when a gateway supplied the response.</summary>
    public static WriteOutcome ClassifyReadFailure(int? status, Exception? exception) =>
        ElasticWriteErrors.IsTransientStatus(status) ? WriteOutcome.Transient : ClassifyRequestFailure(status, exception);

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
