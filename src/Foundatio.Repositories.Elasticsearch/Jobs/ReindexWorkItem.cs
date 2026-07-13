using System;

namespace Foundatio.Repositories.Elasticsearch.Jobs;

public record ReindexWorkItem
{
    public required string OldIndex { get; init; }
    public required string NewIndex { get; init; }
    public required string Alias { get; init; }
    public string? Script { get; init; }
    public bool DeleteOld { get; set; }
    public string? TimestampField { get; init; }
    public DateTime? StartUtc { get; init; }

    /// <summary>
    /// The number of documents Elasticsearch reads and writes per internal bulk batch while reindexing.
    /// Defaults to null, which uses the Elasticsearch reindex API default of 1000. Lower this if reindexing
    /// large documents triggers "rejected execution of coordinating operation" errors from indexing pressure limits.
    /// Must be greater than zero when specified - <see cref="ElasticReindexer.ReindexAsync"/> throws
    /// <see cref="ArgumentOutOfRangeException"/> otherwise.
    /// </summary>
    public int? ReindexBatchSize { get; init; }

    /// <summary>
    /// Throttles the reindex to approximately this many documents per second. Defaults to null, which uses
    /// the Elasticsearch reindex API default of unlimited. Combine with <see cref="ReindexBatchSize"/> to
    /// reduce load on a cluster that is rejecting reindex requests due to indexing pressure limits.
    /// Must be greater than zero when specified - <see cref="ElasticReindexer.ReindexAsync"/> throws
    /// <see cref="ArgumentOutOfRangeException"/> otherwise.
    /// </summary>
    public float? ReindexRequestsPerSecond { get; init; }
}
