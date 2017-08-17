using System;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.Jobs;
using Foundatio.Lock;
using Foundatio.Logging;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Jobs {
    public class ReindexWorkItemHandler : WorkItemHandlerBase {
        private readonly ElasticReindexer _reindexer;
        private readonly ILockProvider _lockProvider;

        public ReindexWorkItemHandler(IElasticClient client, ILockProvider lockProvider, ILoggerFactory loggerFactory = null) {
            _reindexer = new ElasticReindexer(client, loggerFactory.CreateLogger<ReindexWorkItemHandler>());
            _lockProvider = lockProvider;
            AutoRenewLockOnProgress = true;
        }

        public override Task<ILock> GetWorkItemLockAsync(object workItem, CancellationToken cancellationToken = default(CancellationToken)) {
            var reindexWorkItem = workItem as ReindexWorkItem;
            if (reindexWorkItem == null)
                return null;

            return _lockProvider.AcquireAsync(String.Join(":", "reindex", reindexWorkItem.Alias, reindexWorkItem.OldIndex, reindexWorkItem.NewIndex), TimeSpan.FromMinutes(20), cancellationToken);
        }

        public override Task HandleItemAsync(WorkItemContext context) {
            var workItem = context.GetData<ReindexWorkItem>();
            return _reindexer.ReindexAsync(workItem, context.ReportProgressAsync);
        }
    }
}