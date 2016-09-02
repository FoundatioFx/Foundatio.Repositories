using System;
using System.Configuration;
using System.Linq;
using ElasticMacros;
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
    public class MyAppElasticConfiguration : ElasticConfigurationBase {
        public MyAppElasticConfiguration(IQueue<WorkItemData> workItemQueue, ICacheClient cacheClient, IMessageBus messageBus, ILoggerFactory loggerFactory) : base(workItemQueue, cacheClient, messageBus, loggerFactory) {
            // register our custom app query builders
            ElasticQueryBuilder.Default.RegisterDefaults();
            ElasticQueryBuilder.Default.Register(new ElasticMacroSearchQueryBuilder(new ElasticMacroProcessor(c => c.AddAnalyzedField("name"))));
            ElasticQueryBuilder.Default.Register<AgeQueryBuilder>();
            ElasticQueryBuilder.Default.Register<CompanyQueryBuilder>();

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

        public IdentityIndex Identities { get; }
        public EmployeeIndex Employees { get; }
        public DailyLogEventIndex DailyLogEvents { get; }
        public MonthlyLogEventIndex MonthlyLogEvents { get; }
        public ParentChildIndex ParentChild { get; }
    }
}