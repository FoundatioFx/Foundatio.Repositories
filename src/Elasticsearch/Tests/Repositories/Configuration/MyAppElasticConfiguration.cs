using System;
using System.Configuration;
using System.Linq;
using Elasticsearch.Net.ConnectionPool;
using Foundatio.Caching;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Jobs;
using Foundatio.Logging;
using Foundatio.Messaging;
using Foundatio.Queues;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Elasticsearch.Tests.Queries;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Indexes;

namespace Foundatio.Repositories.Elasticsearch.Tests.Configuration {
    public class MyAppElasticConfiguration : ElasticConfiguration {
        public MyAppElasticConfiguration(IQueue<WorkItemData> workItemQueue, ICacheClient cacheClient, IMessageBus messageBus, ILoggerFactory loggerFactory) : base(workItemQueue, cacheClient, messageBus, loggerFactory) {
            AddIndex(Identities = new IdentityIndex(this));
            AddIndex(Employees = new EmployeeIndex(this));
            AddIndex(DailyLogEvents = new DailyLogEventIndex(this));
            AddIndex(MonthlyLogEvents = new MonthlyLogEventIndex(this));
            AddIndex(ParentChild = new ParentChildIndex(this));
        }

        protected override IConnectionPool CreateConnectionPool() {
            var connectionString = ConfigurationManager.ConnectionStrings["ElasticConnectionString"].ConnectionString;
            return new StaticConnectionPool(connectionString.Split(',').Select(url => new Uri(url)));
        }

        public override void ConfigureGlobalQueryBuilders(ElasticQueryBuilder builder) {
            builder.Register<AgeQueryBuilder>();
            builder.Register<CompanyQueryBuilder>();
        }

        public IdentityIndex Identities { get; }
        public EmployeeIndex Employees { get; }
        public DailyLogEventIndex DailyLogEvents { get; }
        public MonthlyLogEventIndex MonthlyLogEvents { get; }
        public ParentChildIndex ParentChild { get; }
    }
}