using System;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.Jobs;
using Foundatio.Lock;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Extensions;

namespace Foundatio.Repositories.Elasticsearch.Jobs {
    public class ReindexWorkItemHandler : WorkItemHandlerBase {
        private readonly IDatabase _database;
        private readonly ILockProvider _lockProvider;

        public ReindexWorkItemHandler(IDatabase database, ILockProvider lockProvider) {
            _database = database;
            _lockProvider = lockProvider;
            AutoRenewLockOnProgress = true;
        }

        public override Task<ILock> GetWorkItemLockAsync(object workItem, CancellationToken cancellationToken = default(CancellationToken)) {
            var reindexWorkItem = workItem as ReindexWorkItem;
            if (reindexWorkItem == null)
                return null;

            return _lockProvider.AcquireAsync(String.Concat("reindex:", reindexWorkItem.Alias, reindexWorkItem.OldIndex, reindexWorkItem.NewIndex), TimeSpan.FromMinutes(20), cancellationToken);
        }

        public override async Task HandleItemAsync(WorkItemContext context) {
            var workItem = context.GetData<ReindexWorkItem>();
            await _database.ReindexAsync(workItem, context.ReportProgressAsync).AnyContext();
        }
    }
}