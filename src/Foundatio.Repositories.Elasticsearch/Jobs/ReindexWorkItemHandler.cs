using System;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.Jobs;
using Foundatio.Lock;
using Microsoft.Extensions.Logging;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Jobs;

public class ReindexWorkItemHandler : WorkItemHandlerBase
{
    private readonly ElasticReindexer _reindexer;
    private readonly ILockProvider _lockProvider;

    public ReindexWorkItemHandler(IElasticClient client, ILockProvider lockProvider, ILoggerFactory? loggerFactory = null) : base(loggerFactory)
    {
        _reindexer = new ElasticReindexer(client, loggerFactory?.CreateLogger<ReindexWorkItemHandler>());
        _lockProvider = lockProvider;
        AutoRenewLockOnProgress = true;
    }

    public override async Task<ILock?> GetWorkItemLockAsync(object workItem, CancellationToken cancellationToken = default)
    {
        if (workItem is not ReindexWorkItem reindexWorkItem)
            return null;

        var resource = string.Join(":", "reindex", reindexWorkItem.Alias, reindexWorkItem.OldIndex, reindexWorkItem.NewIndex);

        try
        {
            return await _lockProvider.AcquireAsync(resource, TimeSpan.FromMinutes(20), true, cancellationToken).ConfigureAwait(false);
        }
        catch (LockAcquisitionTimeoutException)
        {
            return null;
        }
    }

    public override Task HandleItemAsync(WorkItemContext context)
    {
        var workItem = context.GetData<ReindexWorkItem>();
        ArgumentNullException.ThrowIfNull(workItem);

        return _reindexer.ReindexAsync(workItem, context.ReportProgressAsync);
    }
}
