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
/// Registers a single <see cref="VersionedEmployeeIndex"/> at a caller-chosen version, so tests can hand a
/// configuration to <see cref="Jobs.ReindexWorkItemHandler"/> that can resolve the destination index by its
/// versioned name.
/// </summary>
public class VersionedEmployeeElasticConfiguration : ElasticConfiguration
{
    public VersionedEmployeeElasticConfiguration(int version, IQueue<WorkItemData> workItemQueue, ICacheClient cacheClient, IMessageBus messageBus, ILoggerFactory loggerFactory, ILockProvider? lockProvider = null)
        : base(workItemQueue, cacheClient, messageBus, loggerFactory: loggerFactory, lockProvider: lockProvider)
    {
        AddIndex(Employees = new VersionedEmployeeIndex(this, version));
    }

    public VersionedEmployeeIndex Employees { get; }

    protected override NodePool CreateConnectionPool()
    {
        return ElasticTestNodePool.Create();
    }
}
