using System;
using Foundatio.Caching;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Jobs;
using Foundatio.Queues;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Tests.Configuration {
    public class MyAppDatabase : Database {
        public MyAppDatabase(Uri serverUri, IQueue<WorkItemData> workItemQueue, ICacheClient cacheClient) : base(serverUri, workItemQueue, cacheClient) {
            Employee = new EmployeeIndex();
            DailyLogEvent = new DailyLogEventIndex();
            MonthlyLogEvent = new MonthlyLogEventIndex();

            Indexes.Add(Employee);
            Indexes.Add(DailyLogEvent);
            Indexes.Add(MonthlyLogEvent);
        }

        public EmployeeIndex Employee { get; }
        public DailyLogEventIndex DailyLogEvent { get; }
        public MonthlyLogEventIndex MonthlyLogEvent { get; }
    }
}