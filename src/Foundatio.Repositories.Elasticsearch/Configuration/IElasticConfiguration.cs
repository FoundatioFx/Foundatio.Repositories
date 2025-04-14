﻿using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Foundatio.Caching;
using Foundatio.Messaging;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Repositories.Elasticsearch.CustomFields;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Microsoft.Extensions.Logging;

namespace Foundatio.Repositories.Elasticsearch.Configuration;

public interface IElasticConfiguration : IDisposable
{
    ElasticsearchClient Client { get; }
    ICacheClient Cache { get; }
    IMessageBus MessageBus { get; }
    ILoggerFactory LoggerFactory { get; }
    TimeProvider TimeProvider { get; set; }
    IReadOnlyCollection<IIndex> Indexes { get; }
    ICustomFieldDefinitionRepository CustomFieldDefinitionRepository { get; }

    IIndex GetIndex(string name);
    void ConfigureGlobalQueryBuilders(ElasticQueryBuilder builder);
    void ConfigureGlobalQueryParsers(ElasticQueryParserConfiguration config);
    Task ConfigureIndexesAsync(IEnumerable<IIndex> indexes = null, bool beginReindexingOutdated = true);
    Task MaintainIndexesAsync(IEnumerable<IIndex> indexes = null);
    Task DeleteIndexesAsync(IEnumerable<IIndex> indexes = null);
    Task ReindexAsync(IEnumerable<IIndex> indexes = null, Func<int, string, Task> progressCallbackAsync = null);
}
