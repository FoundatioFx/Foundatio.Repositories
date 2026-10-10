using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Foundatio.Caching;
using Foundatio.Jobs;
using Foundatio.Lock;
using Foundatio.Messaging;
using Foundatio.Queues;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Infrastructure;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Indexes;
using Microsoft.Extensions.Logging;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration;

/// <summary>
/// A configuration wired the way production is: it does not call <c>DisableDirectStreaming</c>, so the
/// transport discards raw response bodies.
/// </summary>
/// <remarks>
/// Every other test configuration enables direct-streaming-disabled for readable request logs, which also
/// happens to be what makes reindex failure detection work. That masked a defect where a lossy reindex
/// reported success on any normally-configured client, so at least one test must run without it.
/// </remarks>
public class DirectStreamingElasticConfiguration : ElasticConfiguration
{
    public DirectStreamingElasticConfiguration(int version, IQueue<WorkItemData> workItemQueue, ICacheClient cacheClient, IMessageBus messageBus, ILoggerFactory loggerFactory, ILockProvider? lockProvider = null)
        : base(workItemQueue, cacheClient, messageBus, loggerFactory: loggerFactory, lockProvider: lockProvider)
    {
        AddIndex(Employees = new VersionedEmployeeIndex(this, version));
    }

    public VersionedEmployeeIndex Employees { get; }

    protected override NodePool CreateConnectionPool()
    {
        return ElasticTestNodePool.Create();
    }

    protected override void ConfigureSettings(ElasticsearchClientSettings settings)
    {
        // Deliberately does not disable direct streaming, unlike the other test configurations.
    }
}
