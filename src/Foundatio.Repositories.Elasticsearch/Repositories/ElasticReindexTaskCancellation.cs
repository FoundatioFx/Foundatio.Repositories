using System;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Foundatio.Parsers.ElasticQueries.Extensions;
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
            var response = await client.Tasks.CancelAsync(c => c.TaskId(taskId).WaitForCompletion(), cleanupCancellation.Token).AnyContext();
            logger.LogRequest(response);
            if (!response.IsValidResponse && response.ApiCallDetails.HttpStatusCode is not 404)
                throw new RepositoryException(response.GetErrorMessage($"Unable to confirm termination of reindex task '{taskId.FullyQualifiedId}'."), response.OriginalException());

            var status = await client.Tasks.GetAsync(taskId.FullyQualifiedId, cleanupCancellation.Token).AnyContext();
            if (status.IsValidResponse)
            {
                logger.LogRequest(status);
                if (!status.Completed)
                    throw new RepositoryException($"Reindex task '{taskId.FullyQualifiedId}' remained active after cancellation completed.");
            }
            else if (status.ApiCallDetails.HttpStatusCode is not 404)
            {
                throw new RepositoryException(status.GetErrorMessage($"Unable to verify termination of reindex task '{taskId.FullyQualifiedId}'."), status.OriginalException());
            }
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
