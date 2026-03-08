using System.Collections.Generic;
using System.Linq;
using Nest;

namespace Foundatio.Repositories.Elasticsearch;

internal sealed record BulkResult
{
    public static readonly BulkResult Empty = new();

    public IReadOnlyList<string> SuccessfulIds { get; init; } = [];
    public IReadOnlyList<string> ConflictIds { get; init; } = [];
    public IReadOnlyList<string> RetryableIds { get; init; } = [];
    public IReadOnlyList<string> FatalIds { get; init; } = [];

    public bool IsSuccess => ConflictIds.Count == 0
        && RetryableIds.Count == 0
        && FatalIds.Count == 0;

    public bool HasErrors => !IsSuccess;
    public bool HasConflicts => ConflictIds.Count > 0;
    public bool HasRetryableErrors => RetryableIds.Count > 0;

    internal static BulkResult From(BulkResponse response)
    {
        var errors = response.ItemsWithErrors.ToList();
        return new BulkResult
        {
            SuccessfulIds = response.Items.Where(i => i.IsValid).Select(i => i.Id).ToList(),
            ConflictIds = errors.Where(e => e.Status == 409).Select(e => e.Id).ToList(),
            RetryableIds = errors.Where(e => e.Status is 429 or 503).Select(e => e.Id).ToList(),
            FatalIds = errors.Where(e => e.Status is not 409 and not 429 and not 503).Select(e => e.Id).ToList()
        };
    }
}
