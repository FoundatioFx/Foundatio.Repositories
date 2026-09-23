using System;
using System.Linq;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;

namespace Foundatio.Repositories.Elasticsearch.Extensions;

internal static class SingleAttemptRequest
{
    public static RequestConfigurationDescriptor Configure(ElasticsearchClient client, RequestConfigurationDescriptor request)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(request);
        // Request-local MaxRetries is ignored by the supported transport. Pinning a normal pool-selected
        // node also sets the effective retry count to zero without changing shared client configuration.
        return request.ForceNode(client.ElasticsearchClientSettings.NodePool.CreateView().First().Uri).MaxRetries(0);
    }
}
