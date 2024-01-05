using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Caching;
using Foundatio.Messaging;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Repositories.Elasticsearch.CustomFields;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Microsoft.Extensions.Logging;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Configuration;

public interface IElasticConfiguration : IDisposable
{
    IElasticClient Client { get; }
    ICacheClient Cache { get; }
    IMessageBus MessageBus { get; }
    ILoggerFactory LoggerFactory { get; }
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
