using System;
using System.Collections.Generic;
using System.Linq;
using Elastic.Clients.Elasticsearch;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Extensions;

namespace Foundatio.Repositories.Elasticsearch;

/// <summary>
/// The classified result of one bulk response item. Items are in request order, so callers correlate them with the
/// operations they sent by position, never by id (ids are only unique per index, and the response reports the concrete
/// index even when the request used an alias).
/// </summary>
internal sealed record BulkItemResult(WriteOutcome Outcome, int Status, string? Index, string? Id, string? ErrorType, string? Reason, ElasticDocumentVersion Version)
{
    public WriteItemResult ToWriteResult() => new(Outcome, Status, ErrorType, Reason, null, Version);
}

/// <summary>
/// The classified result of a bulk request. This is the single place bulk responses are classified; callers never
/// re-derive outcomes from the raw response.
/// </summary>
internal sealed class BulkResult
{
    private BulkResult(IReadOnlyList<BulkItemResult> items, WriteItemResult? requestFailure)
    {
        Items = items;
        RequestFailure = requestFailure;
    }

    /// <summary>One result per operation, in request order. Empty when the request failed as a whole.</summary>
    public IReadOnlyList<BulkItemResult> Items { get; }

    /// <summary>Set when the request failed as a whole, so no item results are available.</summary>
    public WriteItemResult? RequestFailure { get; }

    internal static BulkResult From(BulkResponse response)
    {
        // Items is null on a failed request even though it is annotated as non-null.
        if (!response.IsValidResponse && response.Items is null or { Count: 0 })
        {
            int? status = response.ApiCallDetails?.HttpStatusCode;
            var exception = response.OriginalException();
            string? errorType = response.ElasticsearchServerError?.Error?.Type;
            var outcome = WriteOutcomeClassifier.ClassifyRequestFailure(status, exception, errorType);
            return new BulkResult([], new WriteItemResult(outcome, status, errorType, response.GetErrorMessage("Error processing bulk operation"), exception));
        }

        var items = response.Items.Select(item =>
        {
            if (item.IsValid)
            {
                var outcome = item.Status is 404 ? WriteOutcome.Missing : String.Equals(item.Result, "noop", StringComparison.Ordinal) ? WriteOutcome.Noop : WriteOutcome.Succeeded;
                return new BulkItemResult(outcome, item.Status, item.Index, item.Id, null, null, item.GetElasticVersion());
            }

            string? type = item.Error?.Type;
            string? reason = item.Error?.Reason;
            return new BulkItemResult(WriteOutcomeClassifier.Classify(item.Status, type, reason), item.Status, item.Index, item.Id, type, reason, ElasticDocumentVersion.Empty);
        }).ToList();

        return new BulkResult(items, null);
    }

    /// <summary>
    /// Returns one <see cref="WriteItemResult"/> per operation sent, in request order. A request-level failure is applied to
    /// every operation, and a response with fewer items than operations marks the unmatched operations
    /// <see cref="WriteOutcome.Fatal"/>, so no operation is ever left without an outcome.
    /// </summary>
    public IReadOnlyList<WriteItemResult> ToWriteResults(int operationCount)
    {
        if (RequestFailure is { } failure)
            return Enumerable.Repeat(failure, operationCount).ToList();

        var results = new List<WriteItemResult>(operationCount);
        for (int index = 0; index < operationCount; index++)
        {
            results.Add(index < Items.Count
                ? Items[index].ToWriteResult()
                : new WriteItemResult(WriteOutcome.Fatal, Reason: "The bulk response did not contain a result for this operation."));
        }

        return results;
    }
}
