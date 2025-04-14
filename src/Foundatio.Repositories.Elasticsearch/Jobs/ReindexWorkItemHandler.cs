using System;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Foundatio.Jobs;
using Foundatio.Lock;
using Microsoft.Extensions.Logging;

namespace Foundatio.Repositories.Elasticsearch.Jobs;

public class ReindexWorkItemHandler : WorkItemHandlerBase
{
    private readonly ElasticReindexer _reindexer;
    private readonly ILockProvider _lockProvider;

    public ReindexWorkItemHandler(ElasticsearchClient client, ILockProvider lockProvider, ILoggerFactory loggerFactory = null)
    {
        _reindexer = new ElasticReindexer(client, loggerFactory.CreateLogger<ReindexWorkItemHandler>());
        _lockProvider = lockProvider;
        AutoRenewLockOnProgress = true;
    }

    public override Task<ILock> GetWorkItemLockAsync(object workItem, CancellationToken cancellationToken = default)
    {
        if (workItem is not ReindexWorkItem reindexWorkItem)
            return null;

        return _lockProvider.AcquireAsync(String.Join(":", "reindex", reindexWorkItem.Alias, reindexWorkItem.OldIndex, reindexWorkItem.NewIndex), TimeSpan.FromMinutes(20), cancellationToken);
    }

    public override Task HandleItemAsync(WorkItemContext context)
    {
        var workItem = context.GetData<ReindexWorkItem>();
        return _reindexer.ReindexAsync(workItem, context.ReportProgressAsync);
    }
}
