from pathlib import Path
import sys
root = Path(sys.argv[1])
p = root / 'src/Foundatio.Repositories.Elasticsearch/Configuration/ElasticIndexCompatibilityUpgrader.cs'
s = p.read_text()
s = s.replace('.RequestConfiguration(r => r.MaxRetries(0)), cancellationToken)', '.RequestConfiguration(r => SingleAttemptRequest.Configure(_client, r)), cancellationToken)')
p.write_text(s)
p = root / 'src/Foundatio.Repositories.Elasticsearch/Repositories/ElasticReindexTaskRunner.cs'
s = p.read_text()
old = 'd.RequestConfiguration(request => request.OpaqueId(opaqueId).MaxRetries(0));'
assert s.count(old) == 1
s = s.replace(old, 'd.RequestConfiguration(request => SingleAttemptRequest.Configure(_client, request).OpaqueId(opaqueId));')
p.write_text(s)
p = root / 'src/Foundatio.Repositories.Elasticsearch/Extensions/SingleAttemptRequest.cs'
p.write_text('''using System;
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

        // The supported transport ignores request-local MaxRetries when binding configuration. ForceNode
        // also sets the effective retry count to zero; select through the pool's normal live-node view so
        // credentials, TLS, serialization and node selection are retained without mutating shared settings.
        // A failed submission is unknown, not permission to choose another node and submit another task.
        var node = client.ElasticsearchClientSettings.NodePool.CreateView().First();
        return request.ForceNode(node.Uri).MaxRetries(0);
    }
}
''')
for name in ['docs/guide/index-management.md', '.agents/skills/foundatio-repositories/references/index-lifecycle.md']:
    p = root / name
    s = p.read_text()
    s += '\nThe supported Elastic transport does not honor a request-local `MaxRetries(0)` on its own. Non-idempotent compatibility submissions are pinned to one node selected through the pool, which also makes the effective retry count zero. A multi-node regression asserts only one submission occurs despite a globally retrying client. This does not control retries by external proxies.\n'
    p.write_text(s)
