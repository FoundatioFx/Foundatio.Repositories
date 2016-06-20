using System;
using System.Collections.Generic;
using Foundatio.Caching;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Jobs;
using Foundatio.Queues;

namespace Foundatio.Repositories.Elasticsearch.Tests.Configuration {
    public class ElasticConfiguration : ElasticConfigurationBase {
        public ElasticConfiguration(IQueue<WorkItemData> workItemQueue, ICacheClient cacheClient) : base(workItemQueue, cacheClient) {}

        protected override IEnumerable<IIndex> GetIndexes() {
            return new IIndex[] {
                new EmployeeIndex(),
                //new MonthlyEmployeeIndex()
            };
        }
    }
}