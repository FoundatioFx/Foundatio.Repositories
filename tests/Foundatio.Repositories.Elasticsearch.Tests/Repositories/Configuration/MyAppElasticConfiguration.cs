using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.NetworkInformation;
using Elasticsearch.Net;
using Foundatio.Caching;
using Foundatio.Jobs;
using Foundatio.Messaging;
using Foundatio.Queues;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Indexes;
using Microsoft.Extensions.Logging;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration;

public class MyAppElasticConfiguration : ElasticConfiguration
{
    public MyAppElasticConfiguration(IQueue<WorkItemData> workItemQueue, ICacheClient cacheClient, IMessageBus messageBus, ILoggerFactory loggerFactory) : base(workItemQueue, cacheClient, messageBus, loggerFactory)
    {
        AddIndex(Identities = new IdentityIndex(this));
        AddIndex(Employees = new EmployeeIndex(this));
        AddIndex(EmployeeWithCustomFields = new EmployeeWithCustomFieldsIndex(this));
        AddIndex(MonthlyEmployees = new MonthlyEmployeeIndex(this, 1));
        AddIndex(DailyLogEvents = new DailyLogEventIndex(this));
        AddIndex(MonthlyLogEvents = new MonthlyLogEventIndex(this));
        AddIndex(ParentChild = new ParentChildIndex(this));
        AddIndex(MonthlyFileAccessHistory = new MonthlyFileAccessHistoryIndex(this));
        AddCustomFieldIndex(replicas: 0);
    }

    protected override IConnectionPool CreateConnectionPool()
    {
        string connectionString = null;
        bool fiddlerIsRunning = Process.GetProcessesByName("fiddler").Length > 0;

        var servers = new List<Uri>();
        if (!String.IsNullOrEmpty(connectionString))
        {
            servers.AddRange(
                connectionString.Split(',')
                    .Select(url => new Uri(fiddlerIsRunning ? url.Replace("localhost", "ipv4.fiddler") : url)));
        }
        else
        {
            servers.Add(new Uri($"http://{(fiddlerIsRunning ? "ipv4.fiddler" : "elastic.localtest.me")}:9200"));
            if (IsPortOpen(9201))
                servers.Add(new Uri($"http://{(fiddlerIsRunning ? "ipv4.fiddler" : "localhost")}:9201"));
            if (IsPortOpen(9202))
                servers.Add(new Uri($"http://{(fiddlerIsRunning ? "ipv4.fiddler" : "localhost")}:9202"));
        }

        return new StaticConnectionPool(servers);
    }

    private static bool IsPortOpen(int port)
    {
        var ipGlobalProperties = IPGlobalProperties.GetIPGlobalProperties();
        var tcpConnInfoArray = ipGlobalProperties.GetActiveTcpListeners();

        foreach (var endpoint in tcpConnInfoArray)
        {
            if (endpoint.Port == port)
                return true;
        }

        return false;
    }

    protected override IElasticClient CreateElasticClient()
    {
        //var settings = new ConnectionSettings(CreateConnectionPool() ?? new SingleNodeConnectionPool(new Uri("http://localhost:9200")), sourceSerializer: (serializer, values) => new ElasticsearchJsonNetSerializer(serializer, values));
        var settings = new ConnectionSettings(CreateConnectionPool() ?? new SingleNodeConnectionPool(new Uri("http://localhost:9200")));
        settings.EnableApiVersioningHeader();
        ConfigureSettings(settings);
        foreach (var index in Indexes)
            index.ConfigureSettings(settings);

        return new ElasticClient(settings);
    }

    protected override void ConfigureSettings(ConnectionSettings settings)
    {
        // only do this in test and dev mode
        settings.DisableDirectStreaming().PrettyJson();
        base.ConfigureSettings(settings);
    }

    public IdentityIndex Identities { get; }
    public EmployeeIndex Employees { get; }
    public EmployeeWithCustomFieldsIndex EmployeeWithCustomFields { get; }
    public MonthlyEmployeeIndex MonthlyEmployees { get; }
    public DailyLogEventIndex DailyLogEvents { get; }
    public MonthlyLogEventIndex MonthlyLogEvents { get; }
    public ParentChildIndex ParentChild { get; }
    public MonthlyFileAccessHistoryIndex MonthlyFileAccessHistory { get; }
}
