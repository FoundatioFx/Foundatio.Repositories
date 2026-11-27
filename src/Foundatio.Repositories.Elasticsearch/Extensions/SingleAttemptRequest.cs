using System;
using System.Collections.Generic;
using System.Linq;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Elastic.Transport.Diagnostics.Auditing;
using Elastic.Transport.Products.Elasticsearch;
using Foundatio.Repositories.Exceptions;

namespace Foundatio.Repositories.Elasticsearch.Extensions;

internal static class SingleAttemptRequest
{
    internal static bool RejectedBeforeExecution(ElasticsearchResponse response)
    {
        if (FailedBeforeDispatch(response.ApiCallDetails?.AuditTrail))
            return true;
        var error = response.ElasticsearchServerError;
        return (response.ApiCallDetails?.HttpStatusCode, error?.Status, error?.Error?.Type) is
            (400, 400, "action_request_validation_exception") or (403, 403, "security_exception");
    }

    internal static bool FailedBeforeDispatch(IEnumerable<Audit>? trail)
    {
        // A failed ping on this single-node request never submitted the write. An incomplete
        // cancellation audit or any attempted API call keeps the outcome unknown.
        return trail is not null && trail.Any(entry => entry.Event is AuditEvent.PingFailure or AuditEvent.NoNodesAttempted)
            && trail.All(entry => entry.Event is AuditEvent.PingFailure or AuditEvent.NoNodesAttempted
                or AuditEvent.Resurrection or AuditEvent.AllNodesDead or AuditEvent.MaxRetriesReached
                or AuditEvent.FailedOverAllNodes);
    }

    public static RequestConfigurationDescriptor Configure(ElasticsearchClient client, RequestConfigurationDescriptor request)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(request);

        // The supported transport ignores request-local MaxRetries when binding configuration. ForceNode
        // also sets the effective retry count to zero; select through the pool's normal live-node view so
        // credentials, TLS, serialization and node selection are retained without mutating shared settings.
        // A failed submission is unknown, not permission to choose another node and submit another task.
        var settings = client.ElasticsearchClientSettings;
        var predicate = settings.NodePredicate ?? settings.ProductRegistration.NodePredicate;
        var node = settings.ForceNode ?? settings.NodePool.CreateView().FirstOrDefault(predicate)?.Uri
            ?? throw new RepositoryException("No eligible Elasticsearch node is available for a single-dispatch migration request.");
        return request.ForceNode(node).MaxRetries(0);
    }
}
