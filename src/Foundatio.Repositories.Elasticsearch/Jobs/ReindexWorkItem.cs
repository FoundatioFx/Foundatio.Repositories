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
    /// Blocks writes to the source index while the copy is reconciled, then promotes the alias only after the
    /// destination is proven to match. Defaults to <c>false</c>, which preserves the historical ordering where the
    /// alias is promoted before the catch-up pass runs.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>This blocks writes to the source index, and the block lasts until the reindex finishes.</b> The duration
    /// is proportional to how much changed during the copy, but it is not bounded and it is not "brief" on a large
    /// index. Reads are unaffected throughout. Do not enable this without knowing that the application can tolerate
    /// rejected writes (<c>403 cluster_block_exception</c>) for that period.
    /// </para>
    /// <para>
    /// What it buys is that the alias never points at a destination that has not been reconciled. With the default
    /// ordering the alias moves first, which leaves three windows in which live traffic and the catch-up pass
    /// interfere: an update landing after the cutover can be overwritten by the older source copy, a delete can be
    /// resurrected from the source, and - for models with neither a timestamp field nor ObjectId ids - an update to
    /// a pre-existing document is never caught up at all. A blocked source cannot change, so the catch-up pass can
    /// be run to convergence and verified before anything is promoted.
    /// </para>
    /// <para>
    /// This is the only setting that makes a reindex safe for a model whose documents are updated in place. For
    /// append-only data the default ordering is usually sufficient.
    /// </para>
    /// </remarks>
    public bool QuiesceSource { get; init; }
}
