using System;
using System.Linq;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Foundatio.Repositories.Exceptions;

namespace Foundatio.Repositories.Elasticsearch.Extensions;

internal static class SingleNodeDispatchExtensions
{
    internal static RequestConfigurationDescriptor SingleNodeDispatch(this RequestConfigurationDescriptor request, ElasticsearchClient client)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(client);

        var settings = client.ElasticsearchClientSettings;
        var predicate = settings.NodePredicate ?? settings.ProductRegistration.NodePredicate;
        var node = settings.ForceNode ?? settings.NodePool.CreateView().FirstOrDefault(predicate)?.Uri
            ?? throw new RepositoryException("No eligible Elasticsearch node is available for a single-dispatch migration request.");

        // The pinned transport ignores request-local MaxRetries when binding a multi-node pool.
        // ForceNode limits this dispatch to one node without changing retries on the shared client.
        return request.MaxRetries(0).ForceNode(node).DisableSniff();
    }
}
