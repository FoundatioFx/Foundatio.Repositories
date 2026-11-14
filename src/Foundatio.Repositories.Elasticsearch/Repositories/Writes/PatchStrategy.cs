using System;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories.Elasticsearch;

/// <summary>How a patch is executed. Resolved once per call by <see cref="PatchStrategyResolver"/> and used by
/// <c>PatchAsync(id)</c>, <c>PatchAsync(ids)</c> and <c>PatchAllAsync</c>.</summary>
internal enum PatchStrategy
{
    /// <summary>Script and partial patches without a default pipeline: the Update API (bulk <c>update</c> items).
    /// Elasticsearch re-runs the update on the latest document up to <c>Retry(n)</c> times (<c>retry_on_conflict</c>),
    /// or checks <c>IfVersion</c> instead.</summary>
    UpdateApi,

    /// <summary>Script patches with a default pipeline: update-by-query on one id, because the Update API can't run
    /// ingest pipelines. A conflict wrote nothing for that document, so it is retried by re-running the query.</summary>
    UpdateByQueryPerId,

    /// <summary>JSON and action patches, and partial patches with a default pipeline: read, patch in memory and index
    /// with <c>if_seq_no</c>/<c>if_primary_term</c>; conflicts are re-read in grouped rounds.</summary>
    ClientSide,

    /// <summary><c>PatchAllAsync</c> script patches with <c>UseUpdateByQuery()</c> and no cache: one update-by-query
    /// task. Conflicting documents are skipped and reported, never retried (the script may not be idempotent).</summary>
    UpdateByQueryBulk
}

internal static class PatchStrategyResolver
{
    /// <param name="operation">The patch.</param>
    /// <param name="hasDefaultPipeline">Whether the repository has a default ingest pipeline.</param>
    /// <param name="useBulkUpdateByQuery"><c>true</c> only for <c>PatchAllAsync</c> with <c>UseUpdateByQuery()</c> and the cache disabled.</param>
    public static PatchStrategy Resolve<T>(IPatchOperation operation, bool hasDefaultPipeline, bool useBulkUpdateByQuery) where T : class
    {
        return operation switch
        {
            ScriptPatch when useBulkUpdateByQuery => PatchStrategy.UpdateByQueryBulk,
            ScriptPatch when hasDefaultPipeline => PatchStrategy.UpdateByQueryPerId,
            ScriptPatch => PatchStrategy.UpdateApi,
            PartialPatch when hasDefaultPipeline => PatchStrategy.ClientSide,
            PartialPatch => PatchStrategy.UpdateApi,
            JsonPatch or ActionPatch<T> => PatchStrategy.ClientSide,
            _ => throw new ArgumentException("Unknown operation type", nameof(operation))
        };
    }
}
