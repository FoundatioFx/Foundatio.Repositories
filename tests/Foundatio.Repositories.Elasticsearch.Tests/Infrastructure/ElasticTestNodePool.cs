using System;
using System.Linq;
using Elastic.Transport;

namespace Foundatio.Repositories.Elasticsearch.Tests.Infrastructure;

/// <summary>
/// Builds the node pool the test cluster is reached through, honoring the <c>ELASTICSEARCH_URL</c> and
/// <c>USE_FIDDLER_PROXY</c> environment variables. Shared by every test <c>ElasticConfiguration</c> so they
/// all talk to the same cluster.
/// </summary>
public static class ElasticTestNodePool
{
    public static NodePool Create()
    {
        string? connectionString = Environment.GetEnvironmentVariable("ELASTICSEARCH_URL");
        bool fiddlerIsRunning = String.Equals(Environment.GetEnvironmentVariable("USE_FIDDLER_PROXY"), "true", StringComparison.OrdinalIgnoreCase);

        if (!String.IsNullOrEmpty(connectionString))
        {
            var servers = connectionString.Split(',')
                .Select(url => new Uri(fiddlerIsRunning ? url.Replace("localhost", "ipv4.fiddler") : url))
                .ToList();
            return new StaticNodePool(servers);
        }

        string host = fiddlerIsRunning ? "ipv4.fiddler" : "elastic.localtest.me";
        return new SingleNodePool(new Uri($"http://{host}:9200"));
    }
}
