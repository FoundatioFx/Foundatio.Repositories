using System;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Extensions;

namespace Foundatio.Repositories.Elasticsearch.Configuration;

internal static class CompatibilityTargetHealth
{
    public static async Task WaitAsync(ElasticsearchClient client, string targetIndex, TimeProvider timeProvider,
        TimeSpan timeout, Func<Task> renewLeaseAsync, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(timeout, TimeSpan.Zero);
        long started = timeProvider.GetTimestamp();
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var remaining = timeout - timeProvider.GetElapsedTime(started);
            if (remaining <= TimeSpan.Zero)
                throw new RepositoryException($"Compatibility destination '{targetIndex}' did not allocate all configured primary and replica shards within {timeout}. No cutover was authorized.");

            await renewLeaseAsync().AnyContext();
            var pollTimeout = remaining < TimeSpan.FromSeconds(30) ? remaining : TimeSpan.FromSeconds(30);
            var response = await client.Cluster.HealthAsync(d => d
                .Indices(targetIndex)
                .WaitForStatus(HealthStatus.Green)
                .WaitForNoInitializingShards()
                .WaitForNoRelocatingShards()
                .Timeout(pollTimeout)
                .RequestConfiguration(r => r.MaxRetries(0).RequestTimeout(pollTimeout + TimeSpan.FromSeconds(5))), cancellationToken).AnyContext();
            // Elasticsearch returns HTTP 408 for an otherwise valid health observation whose requested
            // status has not been reached yet. Continue bounded polling, never treat this as readiness.
            bool pollTimedOut = response.ApiCallDetails?.HttpStatusCode is 408 && response.TimedOut;
            if (!response.IsValidResponse && !pollTimedOut)
                throw new RepositoryException($"Unable to establish replica readiness for compatibility destination '{targetIndex}'. {response.DebugInformation}");

            if (!response.TimedOut && response.Status is HealthStatus.Green
                && response.UnassignedShards is 0 && response.InitializingShards is 0 && response.RelocatingShards is 0)
                return;
        }
    }
}
