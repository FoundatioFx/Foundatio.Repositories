using System;
using Foundatio.Caching;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Jobs;
using Foundatio.Queues;

namespace Foundatio.Repositories.Elasticsearch.Tests.Configuration {
    public class MyAppDatabase : Database {
        public MyAppDatabase(Uri serverUri, IQueue<WorkItemData> workItemQueue, ICacheClient cacheClient) : base(serverUri, workItemQueue, cacheClient) {
            Employee = new EmployeeIndex(Client);
            DailyLogEvent = new DailyLogEventIndex(Client);
            MonthlyLogEvent = new MonthlyLogEventIndex(Client);

            AddIndex(Employee);
            AddIndex(DailyLogEvent);
            AddIndex(MonthlyLogEvent);
        }

        public EmployeeIndex Employee { get; }
        public DailyLogEventIndex DailyLogEvent { get; }
        public MonthlyLogEventIndex MonthlyLogEvent { get; }
    }
}