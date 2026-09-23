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
    /// Must be a positive, finite number when specified - <see cref="ElasticReindexer.ReindexAsync"/> throws
    /// <see cref="ArgumentOutOfRangeException"/> for zero, negative, <c>NaN</c>, or infinite values.
    /// </summary>
    /// <remarks>
    /// Setting this low enough that Elasticsearch's inter-batch pause (<see cref="ReindexBatchSize"/> divided
    /// by this value) exceeds the reindex's no-progress stall timeout (10 minutes by default) automatically
    /// extends that timeout, so an intentionally slow, throttled reindex isn't mistaken for a stalled one and
    /// cancelled. See <see cref="ElasticReindexer.GetNoProgressTimeout"/>.
    /// </remarks>
    public float? ReindexRequestsPerSecond { get; init; }

    /// <summary>
    /// Reconciles an unscripted copy behind a source write fence before alias promotion. Defaults to false.
    /// Nonempty transformation scripts are rejected before any Elasticsearch request in this mode.
    /// </summary>
    /// <remarks>
    /// The blocked reconciliation scans the whole source and destination, so outage cost is not delta-only.
    /// Gate application writers and drain accepted work first. Routing and complete item/shard evidence are
    /// required, and both count deficits and surpluses refuse promotion. Retained source generations stay
    /// blocked on success or failure until deletion or deliberate operator recovery. A lost cutover response
    /// does not establish which generation serves traffic; a work-item flag does not prove completion.
    /// This does not implement online change replay, automatic crash takeover, or storage-enforced fencing.
    /// </remarks>
    public bool QuiesceSource { get; init; }
}
