using System;
using System.Collections.Generic;
using System.Linq;
using Elastic.Clients.Elasticsearch;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Extensions;

namespace Foundatio.Repositories.Elasticsearch;

internal sealed record BulkResult
{
    private static readonly IReadOnlySet<string> _emptySet = new HashSet<string>();

    public static readonly BulkResult Empty = new();

    public IReadOnlySet<string> SuccessfulIds { get; init; } = _emptySet;
    public IReadOnlySet<string> NoopIds { get; init; } = _emptySet;
    public IReadOnlySet<string> ConflictIds { get; init; } = _emptySet;
    public IReadOnlySet<string> RetryableIds { get; init; } = _emptySet;
    public IReadOnlySet<string> FatalIds { get; init; } = _emptySet;

    public long ModifiedCount => SuccessfulIds.Count - NoopIds.Count;

    public string? TransportError { get; init; }
    public Exception? TransportException { get; init; }

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
        var validItems = response.Items.Where(i => i.IsValid && i.Id is not null).ToList();
        return new BulkResult
        {
            SuccessfulIds = validItems.Select(i => i.Id).OfType<string>().ToHashSet(),
            NoopIds = validItems.Where(i => String.Equals(i.Result, "noop", StringComparison.Ordinal)).Select(i => i.Id).OfType<string>().ToHashSet(),
            ConflictIds = errors.Where(e => e.Status is 409 && e.Id is not null).Select(e => e.Id).OfType<string>().ToHashSet(),
            RetryableIds = errors.Where(e => e.Status is 429 or 503 && e.Id is not null).Select(e => e.Id).OfType<string>().ToHashSet(),
            FatalIds = errors.Where(e => e.Status is not 409 and not 429 and not 503 && e.Id is not null).Select(e => e.Id).OfType<string>().ToHashSet()
        };
    }
}
