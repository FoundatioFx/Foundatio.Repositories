using System;
using System.Collections.Generic;
using System.Linq;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Exceptions;
using Nest;

namespace Foundatio.Repositories.Elasticsearch;

internal sealed record BulkResult
{
    public static readonly BulkResult Empty = new();

    public IReadOnlySet<string> SuccessfulIds { get; init; } = new HashSet<string>();
    public IReadOnlySet<string> ConflictIds { get; init; } = new HashSet<string>();
    public IReadOnlySet<string> RetryableIds { get; init; } = new HashSet<string>();
    public IReadOnlySet<string> FatalIds { get; init; } = new HashSet<string>();

    public bool IsSuccess => ConflictIds.Count is 0
        && RetryableIds.Count is 0
        && FatalIds.Count is 0;

    public bool HasErrors => !IsSuccess;
    public bool HasConflicts => ConflictIds.Count > 0;
    public bool HasRetryableErrors => RetryableIds.Count > 0;

    internal static BulkResult From(BulkResponse response)
    {
        if (!response.IsValid && !response.ItemsWithErrors.Any())
            throw new DocumentException(response.GetErrorMessage("Error processing bulk operation"), response.OriginalException);

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
