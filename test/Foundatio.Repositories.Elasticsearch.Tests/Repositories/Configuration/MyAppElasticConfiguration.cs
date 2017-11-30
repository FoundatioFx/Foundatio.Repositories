using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Net.Sockets;
using Elasticsearch.Net;
using Foundatio.Caching;
using Foundatio.Jobs;
using Foundatio.Messaging;
using Foundatio.Queues;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Indexes;
using Microsoft.Extensions.Logging;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration {
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
            string connectionString = null;
            bool fiddlerIsRunning = Process.GetProcessesByName("fiddler").Length > 0;

            var servers = new List<Uri>();
            if (!String.IsNullOrEmpty(connectionString)) {
                servers.AddRange(
                    connectionString.Split(',')
                        .Select(url => new Uri(fiddlerIsRunning ? url.Replace("localhost", "ipv4.fiddler") : url)));
            } else {
                servers.Add(new Uri($"http://{(fiddlerIsRunning ? "ipv4.fiddler" : "localhost")}:9200"));
                if (IsPortOpen(9201))
                    servers.Add(new Uri($"http://{(fiddlerIsRunning ? "ipv4.fiddler" : "localhost")}:9201"));
                if (IsPortOpen(9202))
                    servers.Add(new Uri($"http://{(fiddlerIsRunning ? "ipv4.fiddler" : "localhost")}:9202"));
            }

            return new StaticConnectionPool(servers);
        }

        private bool IsPortOpen(int port) {
            using (var tcpClient = new TcpClient()) {
                try {
                    tcpClient.Connect("127.0.0.1", port);
                    return true;
                } catch (Exception) {
                    return false;
                }
            }
        }

        protected override void ConfigureSettings(ConnectionSettings settings) {
            // only do this in test and dev mode
            settings.DisableDirectStreaming().PrettyJson();
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