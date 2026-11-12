using System.Collections.Generic;
using Elastic.Clients.Elasticsearch;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Utility;

namespace Foundatio.Repositories.Elasticsearch;

/// <summary>
/// One multi-get item, in request order: a found document, a document that doesn't exist, or an item-level error
/// (for example an unavailable shard). An error is never treated as "not found".
/// </summary>
internal readonly record struct MultiGetItem<T>(string Id, string? Index, FindHit<T>? Hit, string? ErrorType, string? ErrorReason) where T : class
{
    public bool IsError => ErrorType is not null || ErrorReason is not null;
    public bool IsFound => Hit is not null;
}

internal static class MultiGetItems
{
    /// <summary>
    /// Returns one item per requested document, in request order. A missing index (<c>index_not_found_exception</c>) is
    /// reported as not found, because the document can't exist there; every other item error is reported as an error.
    /// </summary>
    public static IReadOnlyList<MultiGetItem<T>> ToMultiGetItems<T>(this MultiGetResponse<T> response) where T : class
    {
        var items = new List<MultiGetItem<T>>(response.Docs.Count);
        foreach (var doc in response.Docs)
        {
            MultiGetItem<T> item = default;
            doc.Match(
                result =>
                {
                    if (result is null)
                        return;

                    FindHit<T>? hit = null;
                    if (result.Found)
                    {
                        var version = ElasticIndexExtensions.ToElasticVersion(result.PrimaryTerm, result.SeqNo);
                        result.Source.SetDocumentVersion(version);
                        hit = new FindHit<T>(result.Id, result.Source, 0, version.ToVersionString(), result.Routing, new DataDictionary { { ElasticDataKeys.Index, result.Index } });
                    }

                    item = new MultiGetItem<T>(result.Id, result.Index, hit, null, null);
                },
                error =>
                {
                    if (error is null)
                        return;

                    item = error.Error?.Type is "index_not_found_exception"
                        ? new MultiGetItem<T>(error.Id, error.Index, null, null, null)
                        : new MultiGetItem<T>(error.Id, error.Index, null, error.Error?.Type ?? "unknown", error.Error?.Reason);
                });

            items.Add(item);
        }

        return items;
    }
}
