using System;
using System.Diagnostics;
using System.Linq;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Foundatio.Caching;
using Foundatio.Jobs;
using Foundatio.Messaging;
using Foundatio.Queues;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.CustomFields;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Indexes;
using Microsoft.Extensions.Logging;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration;

public class MyAppElasticConfiguration : ElasticConfiguration
{
    public MyAppElasticConfiguration(IQueue<WorkItemData> workItemQueue, ICacheClient cacheClient, IMessageBus messageBus, ILoggerFactory loggerFactory)
        : base(workItemQueue, cacheClient, messageBus, null, null, loggerFactory)
    {
        AddIndex(Identities = new IdentityIndex(this));
        AddIndex(Employees = new EmployeeIndex(this));
        AddIndex(EmployeeWithCustomFields = new EmployeeWithCustomFieldsIndex(this));
        AddIndex(MonthlyEmployees = new MonthlyEmployeeIndex(this, 1));
        AddIndex(DailyLogEvents = new DailyLogEventIndex(this));
        AddIndex(MonthlyLogEvents = new MonthlyLogEventIndex(this));
        AddIndex(ParentChild = new ParentChildIndex(this));
        AddIndex(DailyFileAccessHistory = new DailyFileAccessHistoryIndex(this));
        AddIndex(MonthlyFileAccessHistory = new MonthlyFileAccessHistoryIndex(this));
        CustomFields = AddCustomFieldIndex(replicas: 0);
    }

    protected override NodePool CreateConnectionPool()
    {
        string connectionString = Environment.GetEnvironmentVariable("ELASTICSEARCH_URL");
        bool fiddlerIsRunning = Process.GetProcessesByName("fiddler").Length > 0;

        if (!String.IsNullOrEmpty(connectionString))
        {
            var servers = connectionString.Split(',')
                .Select(url => new Uri(fiddlerIsRunning ? url.Replace("localhost", "ipv4.fiddler") : url))
                .ToList();
            return new StaticNodePool(servers);
        }

        var host = fiddlerIsRunning ? "ipv4.fiddler" : "elastic.localtest.me";
        return new SingleNodePool(new Uri($"http://{host}:9200"));
    }

    protected override ElasticsearchClient CreateElasticClient()
    {
        var settings = new ElasticsearchClientSettings(CreateConnectionPool() ?? new SingleNodePool(new Uri("http://localhost:9200")));
        ConfigureSettings(settings);
        foreach (var index in Indexes)
            index.ConfigureSettings(settings);

        return new ElasticsearchClient(settings);
    }

    protected override void ConfigureSettings(ElasticsearchClientSettings settings)
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
    public DailyFileAccessHistoryIndex DailyFileAccessHistory { get; }
    public MonthlyFileAccessHistoryIndex MonthlyFileAccessHistory { get; }
    public CustomFieldDefinitionIndex CustomFields { get; }
}
