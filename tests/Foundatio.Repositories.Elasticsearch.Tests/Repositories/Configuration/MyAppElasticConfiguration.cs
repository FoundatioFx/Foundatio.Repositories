using System;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using Elasticsearch.Net;
using Foundatio.Caching;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Jobs;
using Foundatio.Logging;
using Foundatio.Messaging;
using Foundatio.Queues;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Indexes;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Tests.Configuration {
    public class MyAppElasticConfiguration : ElasticConfiguration {
        public MyAppElasticConfiguration(IQueue<WorkItemData> workItemQueue, ICacheClient cacheClient, IMessageBus messageBus, ILoggerFactory loggerFactory) : base(workItemQueue, cacheClient, messageBus, loggerFactory) {
            AddIndex(Identities = new IdentityIndex(this));
            AddIndex(Employees = new EmployeeIndex(this));
            AddIndex(MonthlyEmployees = new MonthlyEmployeeIndex(this, 1));
            AddIndex(DailyLogEvents = new DailyLogEventIndex(this));
            AddIndex(MonthlyLogEvents = new MonthlyLogEventIndex(this));
            AddIndex(ParentChild = new ParentChildIndex(this));
        }

        protected override IConnectionPool CreateConnectionPool() {
            var connectionString = ConfigurationManager.ConnectionStrings["ElasticConnectionString"].ConnectionString;
            bool fiddlerIsRunning = Process.GetProcessesByName("fiddler").Length > 0;
            return new StaticConnectionPool(connectionString.Split(',')
                .Select(url => new Uri(fiddlerIsRunning ? url.Replace("localhost", "ipv4.fiddler") : url)));
        }

        protected override void ConfigureSettings(ConnectionSettings settings) {
            // Allow us to log out the responses.
            settings.DisableDirectStreaming(); //.PrettyJson();
            base.ConfigureSettings(settings);
        }

        public IdentityIndex Identities { get; }
        public EmployeeIndex Employees { get; }
        public MonthlyEmployeeIndex MonthlyEmployees { get; }
        public DailyLogEventIndex DailyLogEvents { get; }
        public MonthlyLogEventIndex MonthlyLogEvents { get; }
        public ParentChildIndex ParentChild { get; }
    }
}