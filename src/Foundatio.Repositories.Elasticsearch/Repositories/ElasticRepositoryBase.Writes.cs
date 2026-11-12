using System;
using System.Net.Http;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Elastic.Transport.Products.Elasticsearch;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Options;
using Foundatio.Resilience;
using Microsoft.Extensions.Logging;

namespace Foundatio.Repositories.Elasticsearch;

public abstract partial class ElasticRepositoryBase<T>
{
    private static readonly TimeSpan _retryBaseDelay = TimeSpan.FromMilliseconds(50);
    private static readonly TimeSpan _retryMaxDelay = TimeSpan.FromSeconds(2);

    /// <summary>
    /// Builds the policy for a single-document write. <c>Retry(n)</c> means <c>n</c> retries, so the policy makes
    /// <c>n + 1</c> attempts. Transient errors (HTTP 429, 502, 503, 504 and transport failures) are always retried. Version
    /// conflicts are retried only when <paramref name="retryConflicts"/> is <c>true</c> (read, re-apply and write patches).
    /// </summary>
    private IResiliencePolicy CreateWritePolicy(ICommandOptions options, bool retryConflicts)
    {
        int maxAttempts = options.GetRetryCount() + 1;
        var basePolicy = _resiliencePolicy as ResiliencePolicy ?? new ResiliencePolicy(_logger, ElasticIndex.Configuration.TimeProvider);
        var policy = basePolicy.Clone(maxAttempts, getDelay: GetRetryDelay);
        var configuredShouldRetry = basePolicy.ShouldRetry;

        policy.MaxDelay = _retryMaxDelay;
        policy.UseJitter = true;
        policy.ShouldRetry = (attempt, ex) =>
        {
            if (!IsRetryableWriteException(ex, retryConflicts))
                return false;

            return configuredShouldRetry?.Invoke(attempt, ex) ?? true;
        };

        return policy;
    }

    private static bool IsRetryableWriteException(Exception ex, bool retryConflicts)
    {
        return ex switch
        {
            TransientDocumentException => true,
            VersionConflictDocumentException => retryConflicts,
            HttpRequestException => true,
            TransportException => true,
            _ => false
        };
    }

    /// <summary>
    /// Exponential backoff starting at 50 ms and capped at 2 seconds. Jitter is added by the policy or by the caller.
    /// </summary>
    private static TimeSpan GetRetryDelay(int attempt)
    {
        double milliseconds = _retryBaseDelay.TotalMilliseconds * Math.Pow(2, Math.Max(0, attempt - 1));
        return TimeSpan.FromMilliseconds(Math.Min(milliseconds, _retryMaxDelay.TotalMilliseconds));
    }

    private static TimeSpan GetRetryDelayWithJitter(int attempt)
    {
        var delay = GetRetryDelay(attempt);
        double jitter = delay.TotalMilliseconds * 0.25 * ((Random.Shared.NextDouble() * 2) - 1);
        return TimeSpan.FromMilliseconds(Math.Max(0, delay.TotalMilliseconds + jitter));
    }

    /// <summary>
    /// Maps a failed single-document write response to the exception callers should see.
    /// </summary>
    private Exception CreateWriteException(ElasticsearchResponse response, string operation, string index, string? id, bool isCreateOperation = false)
    {
        int? status = response.ApiCallDetails?.HttpStatusCode ?? response.ElasticsearchServerError?.Status;
        string? errorType = response.ElasticsearchServerError?.Error?.Type;
        string message = response.GetErrorMessage($"Error {operation} document {index}/{id}");

        if (ElasticWriteErrors.IsVersioningNotSupported(errorType))
            return new DocumentVersioningNotSupportedException(ElasticWriteErrors.VersioningNotSupportedMessage(index), response.OriginalException());

        if (status is 409)
        {
            if (isCreateOperation)
                return new DuplicateDocumentException(message, response.OriginalException());

            return new VersionConflictDocumentException(message, response.OriginalException());
        }

        if (status is null || ElasticWriteErrors.IsTransientStatus(status))
            return new TransientDocumentException(message, status, response.OriginalException());

        return new DocumentException(message, response.OriginalException());
    }

    /// <summary>
    /// Throws a <see cref="DocumentException"/> when a version-checked write is about to run without a
    /// version (for example the document came from an index with sequence numbers disabled, or its version was never set).
    /// Without this guard the write would run as an unconditional overwrite.
    /// </summary>
    private static void ThrowIfMissingVersion(ElasticDocumentVersion version, string index, string? id)
    {
        if (!version.IsEmpty)
            return;

        throw new DocumentException($"Document {index}/{id} has no version, so an optimistic concurrency check is not possible. Load the document before saving it, use SkipVersionCheck to overwrite it, or check that index {index} has sequence numbers enabled.");
    }

    private void LogVersionConflict(string operation, string? index, string? id, int attempt, int budget, string? expectedVersion, string? reason, string outcome)
    {
        if (!_logger.IsEnabled(LogLevel.Warning))
            return;

        var (parsedExpected, current) = ElasticWriteErrors.ParseConflictVersions(reason);
        _logger.LogWarning("Version conflict during {Operation} on {Index}/{DocumentId}: attempt {Attempt} of {Budget}, expected version {ExpectedVersion}, current version {CurrentVersion}, outcome {Outcome}",
            operation, index, id, attempt, budget + 1, expectedVersion ?? parsedExpected, current, outcome);
    }
}
