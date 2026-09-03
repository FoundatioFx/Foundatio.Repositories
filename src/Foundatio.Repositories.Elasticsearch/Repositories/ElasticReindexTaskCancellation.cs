using System;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Extensions;
using Microsoft.Extensions.Logging;

namespace Foundatio.Repositories.Elasticsearch;

internal static class ElasticReindexTaskCancellation
{
    public static async Task CancelAndConfirmAsync(
        ElasticsearchClient client,
        ILogger logger,
        TaskId taskId,
        string sourceIndex,
        string targetIndex,
        Exception taskException)
    {
        try
        {
            using var cleanupCancellation = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            var response = await client.Tasks.CancelAsync(taskId, d => d.WaitForCompletion(), cleanupCancellation.Token).AnyContext();
            if (response.IsValidResponse || response.ApiCallDetails.HttpStatusCode is 404)
                logger.LogRequest(response);
            else
            {
                logger.LogErrorRequest(response, "Unable to cancel reindex task {TaskId} for {SourceIndex} -> {TargetIndex}", taskId.FullyQualifiedId, sourceIndex, targetIndex);
                throw new RepositoryException(
                    $"Unable to confirm termination of reindex task '{taskId.FullyQualifiedId}'. {response.DebugInformation}",
                    response.OriginalException());
            }

            // A partial cancellation response (node_failures/task_failures) means some nodes were never asked to
            // stop, so this response alone cannot prove termination. Elasticsearch also reports failed-node
            // entries when the task finished before the cancel arrived, so a partial response is only recorded
            // here; termination itself is decided exclusively by the authoritative task status read below.
            bool partialCancellation = response.NodeFailures is { Count: > 0 }
                || response.TaskFailures is { Count: > 0 };

            var status = await client.Tasks.GetAsync(taskId.FullyQualifiedId, cleanupCancellation.Token).AnyContext();
            if (status.ApiCallDetails.HttpStatusCode is 404)
            {
                logger.LogRequest(status);
                return;
            }

            if (status.IsValidResponse)
            {
                logger.LogRequest(status);
                if (!status.Completed)
                    throw new RepositoryException($"Reindex task '{taskId.FullyQualifiedId}' remained active after cancellation completed.{(partialCancellation ? " The cancellation request reported partial node or task failures." : String.Empty)}");

                return;
            }

            logger.LogErrorRequest(status, "Unable to verify termination of reindex task {TaskId} for {SourceIndex} -> {TargetIndex}", taskId.FullyQualifiedId, sourceIndex, targetIndex);

            throw new RepositoryException(
                $"Unable to verify termination of reindex task '{taskId.FullyQualifiedId}'.{(partialCancellation ? " The cancellation request reported partial node or task failures." : String.Empty)}",
                status.OriginalException() ?? new RepositoryException(status.DebugInformation));
        }
        catch (Exception cancellationException)
        {
            logger.LogWarning(cancellationException, "Unable to confirm termination of reindex task {TaskId} for {SourceIndex} -> {TargetIndex}", taskId.FullyQualifiedId, sourceIndex, targetIndex);
            throw new ElasticReindexTaskUncertainException(
                $"Reindex task '{taskId.FullyQualifiedId}' for '{sourceIndex}' -> '{targetIndex}' may still be running. Do not delete either index until the task is confirmed terminated.",
                new AggregateException(taskException, cancellationException));
        }
    }

}
