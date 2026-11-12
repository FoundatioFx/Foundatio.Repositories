using System;
using System.Collections.Generic;
using System.Linq;
using Elastic.Clients.Elasticsearch;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Extensions;

namespace Foundatio.Repositories.Elasticsearch;

/// <summary>
/// The error reported for one bulk item.
/// </summary>
internal sealed record BulkItemError(int Status, string? Type, string? Reason, string? Index);

internal sealed record BulkResult
{
    private static readonly IReadOnlySet<string> _emptySet = new HashSet<string>();
    private static readonly IReadOnlyDictionary<string, BulkItemError> _emptyErrors = new Dictionary<string, BulkItemError>();

    public static readonly BulkResult Empty = new();

    public IReadOnlySet<string> SuccessfulIds { get; init; } = _emptySet;
    public IReadOnlySet<string> NoopIds { get; init; } = _emptySet;

    /// <summary>Items that failed with HTTP 409 because the document changed (not because it is missing).</summary>
    public IReadOnlySet<string> ConflictIds { get; init; } = _emptySet;

    /// <summary>Items that failed with HTTP 409 because a version-checked write found no document (deleted since it was read).</summary>
    public IReadOnlySet<string> MissingIds { get; init; } = _emptySet;

    /// <summary>Items that failed with a transient status (HTTP 429, 502, 503 or 504).</summary>
    public IReadOnlySet<string> RetryableIds { get; init; } = _emptySet;

    /// <summary>Items that failed because the index has sequence numbers disabled.</summary>
    public IReadOnlySet<string> VersioningNotSupportedIds { get; init; } = _emptySet;

    public IReadOnlySet<string> FatalIds { get; init; } = _emptySet;

    public IReadOnlyDictionary<string, BulkItemError> Errors { get; init; } = _emptyErrors;

    public long ModifiedCount => SuccessfulIds.Count - NoopIds.Count;

    public string? TransportError { get; init; }
    public Exception? TransportException { get; init; }
    public int? TransportStatusCode { get; init; }

    public bool HasTransportError => TransportError is not null;

    public bool IsSuccess => !HasTransportError
        && ConflictIds.Count is 0
        && RetryableIds.Count is 0
        && FatalIds.Count is 0
        && VersioningNotSupportedIds.Count is 0;

    public bool HasErrors => !IsSuccess;
    public bool HasConflicts => ConflictIds.Count > 0;
    public bool HasRetryableErrors => RetryableIds.Count > 0;

    internal static BulkResult From(BulkResponse response)
    {
        if (!response.IsValidResponse && !response.ItemsWithErrors.Any())
        {
            return new BulkResult
            {
                TransportError = response.GetErrorMessage("Error processing bulk operation"),
                TransportException = response.OriginalException(),
                TransportStatusCode = response.ApiCallDetails?.HttpStatusCode
            };
        }

        var errors = new Dictionary<string, BulkItemError>();
        foreach (var item in response.ItemsWithErrors)
        {
            if (item.Id is null)
                continue;

            errors[item.Id] = new BulkItemError(item.Status, item.Error?.Type, item.Error?.Reason, item.Index);
        }

        var validItems = response.Items.Where(i => i.IsValid && i.Id is not null).ToList();
        return new BulkResult
        {
            SuccessfulIds = validItems.Select(i => i.Id).OfType<string>().ToHashSet(),
            NoopIds = validItems.Where(i => String.Equals(i.Result, "noop", StringComparison.Ordinal)).Select(i => i.Id).OfType<string>().ToHashSet(),
            ConflictIds = errors.Where(e => e.Value.Status is 409 && !ElasticWriteErrors.IsMissingDocumentConflict(e.Value.Reason)).Select(e => e.Key).ToHashSet(),
            MissingIds = errors.Where(e => e.Value.Status is 409 && ElasticWriteErrors.IsMissingDocumentConflict(e.Value.Reason)).Select(e => e.Key).ToHashSet(),
            RetryableIds = errors.Where(e => ElasticWriteErrors.IsTransientStatus(e.Value.Status)).Select(e => e.Key).ToHashSet(),
            VersioningNotSupportedIds = errors.Where(e => ElasticWriteErrors.IsVersioningNotSupported(e.Value.Type)).Select(e => e.Key).ToHashSet(),
            FatalIds = errors.Where(e => e.Value.Status is not 409 && !ElasticWriteErrors.IsTransientStatus(e.Value.Status) && !ElasticWriteErrors.IsVersioningNotSupported(e.Value.Type)).Select(e => e.Key).ToHashSet(),
            Errors = errors
        };
    }
}
