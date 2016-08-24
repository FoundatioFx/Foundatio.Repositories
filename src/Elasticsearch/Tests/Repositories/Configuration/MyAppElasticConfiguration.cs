using System;
using System.Collections.Generic;
using System.Configuration;
using Elasticsearch.Net.ConnectionPool;
using Foundatio.Caching;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Jobs;
using Foundatio.Logging;
using Foundatio.Queues;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Elasticsearch.Tests.Queries;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Indexes;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Tests.Configuration {
    public class MyAppElasticConfiguration : ElasticConfiguration {
        public MyAppElasticConfiguration(IQueue<WorkItemData> workItemQueue, ICacheClient cacheClient, ILoggerFactory loggerFactory) : base(null, workItemQueue, cacheClient, loggerFactory) {
            var connectionString = ConfigurationManager.ConnectionStrings["ElasticConnectionString"].ConnectionString;
            Client = new ElasticClient(new ConnectionSettings(new StaticConnectionPool(new List<Uri> { new Uri(connectionString) })).EnableTcpKeepAlive(30 * 1000, 2000));

            // register our custom app query builders
            ElasticQueryBuilder.Default.RegisterDefaults();
            ElasticQueryBuilder.Default.Register<AgeQueryBuilder>();
            ElasticQueryBuilder.Default.Register<CompanyQueryBuilder>();

            Identities = new IdentityIndex(Client, cacheClient, loggerFactory);
            Employees = new EmployeeIndex(Client, cacheClient, loggerFactory);
            DailyLogEvents = new DailyLogEventIndex(Client, cacheClient, loggerFactory);
            MonthlyLogEvents = new MonthlyLogEventIndex(Client, cacheClient, loggerFactory);
            ParentChild = new ParentChildIndex(Client, cacheClient, loggerFactory);

            AddIndex(Identities);
            AddIndex(Employees);
            AddIndex(DailyLogEvents);
            AddIndex(MonthlyLogEvents);
            AddIndex(ParentChild);
        }

        public IdentityIndex Identities { get; }
        public EmployeeIndex Employees { get; }
        public DailyLogEventIndex DailyLogEvents { get; }
        public MonthlyLogEventIndex MonthlyLogEvents { get; }
        public ParentChildIndex ParentChild { get; }
    }
}