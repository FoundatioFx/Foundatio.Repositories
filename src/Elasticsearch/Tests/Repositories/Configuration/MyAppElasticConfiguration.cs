using System;
using System.Configuration;
using Foundatio.Caching;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Jobs;
using Foundatio.Logging;
using Foundatio.Queues;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Elasticsearch.Tests.Queries;

namespace Foundatio.Repositories.Elasticsearch.Tests.Configuration {
    public class MyAppElasticConfiguration : ElasticConfiguration {
        public MyAppElasticConfiguration(IQueue<WorkItemData> workItemQueue, ICacheClient cacheClient, ILoggerFactory loggerFactory) : base(workItemQueue, cacheClient, loggerFactory) {
            var connectionString = ConfigurationManager.ConnectionStrings["ElasticConnectionString"].ConnectionString;
            SetClient(new Uri(connectionString));

            // register our custom app query builders
            ElasticQueryBuilder.Default.RegisterDefaults();
            ElasticQueryBuilder.Default.Register<AgeQueryBuilder>();
            ElasticQueryBuilder.Default.Register<CompanyQueryBuilder>();

            Identities = new IdentityIndex(Client, loggerFactory);
            Employees = new EmployeeIndex(Client, loggerFactory);
            DailyLogEvents = new DailyLogEventIndex(Client, loggerFactory);
            MonthlyLogEvents = new MonthlyLogEventIndex(Client, loggerFactory);

            AddIndex(Identities);
            AddIndex(Employees);
            AddIndex(DailyLogEvents);
            AddIndex(MonthlyLogEvents);
        }

        public IdentityIndex Identities { get; }
        public EmployeeIndex Employees { get; }
        public DailyLogEventIndex DailyLogEvents { get; }
        public MonthlyLogEventIndex MonthlyLogEvents { get; }
    }
}