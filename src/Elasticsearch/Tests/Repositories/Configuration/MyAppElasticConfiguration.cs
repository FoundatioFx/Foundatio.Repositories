using System;
using Foundatio.Caching;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Jobs;
using Foundatio.Logging;
using Foundatio.Queues;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Elasticsearch.Tests.Queries;

namespace Foundatio.Repositories.Elasticsearch.Tests.Configuration {
    public class MyAppElasticConfiguration : ElasticConfiguration {
        public MyAppElasticConfiguration(Uri serverUri, IQueue<WorkItemData> workItemQueue, ICacheClient cacheClient, ILogger logger) : base(serverUri, workItemQueue, cacheClient, logger) {
            // register our custom app query builders
            ElasticQueryBuilder.Default.Register<AgeQueryBuilder>();
            ElasticQueryBuilder.Default.Register<CompanyQueryBuilder>();

            Employees = new EmployeeIndex(Client);
            DailyLogEvents = new DailyLogEventIndex(Client);
            MonthlyLogEvents = new MonthlyLogEventIndex(Client);

            AddIndex(Employees);
            AddIndex(DailyLogEvents);
            AddIndex(MonthlyLogEvents);
        }

        public EmployeeIndex Employees { get; }
        public DailyLogEventIndex DailyLogEvents { get; }
        public MonthlyLogEventIndex MonthlyLogEvents { get; }
    }
}