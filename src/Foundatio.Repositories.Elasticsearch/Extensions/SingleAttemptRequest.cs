using System;
using System.Linq;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Foundatio.Repositories.Exceptions;

namespace Foundatio.Repositories.Elasticsearch.Extensions;

internal static class SingleAttemptRequest
{
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
