using System;
using System.Collections.Generic;
using System.Linq;
using Elastic.Clients.Elasticsearch;
using Foundatio.Repositories.Elasticsearch.Extensions;

namespace Foundatio.Repositories.Elasticsearch;

internal sealed record BulkResult
{
    private static readonly IReadOnlySet<string> _emptySet = new HashSet<string>();

    public static readonly BulkResult Empty = new();

    public IReadOnlySet<string> SuccessfulIds { get; init; } = _emptySet;
    public IReadOnlySet<string> ConflictIds { get; init; } = _emptySet;
    public IReadOnlySet<string> RetryableIds { get; init; } = _emptySet;
    public IReadOnlySet<string> FatalIds { get; init; } = _emptySet;

    public string TransportError { get; init; }
    public Exception TransportException { get; init; }

    public bool HasTransportError => TransportError is not null;

    public bool IsSuccess => !HasTransportError
        && ConflictIds.Count is 0
        && RetryableIds.Count is 0
        && FatalIds.Count is 0;

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
                TransportException = response.OriginalException()
            };
        }

        var errors = response.ItemsWithErrors.ToList();
        return new BulkResult
        {
            SuccessfulIds = response.Items.Where(i => i.IsValid).Select(i => i.Id).ToHashSet(),
            ConflictIds = errors.Where(e => e.Status is 409).Select(e => e.Id).ToHashSet(),
            RetryableIds = errors.Where(e => e.Status is 429 or 503).Select(e => e.Id).ToHashSet(),
            FatalIds = errors.Where(e => e.Status is not 409 and not 429 and not 503).Select(e => e.Id).ToHashSet()
        };
    }
}
