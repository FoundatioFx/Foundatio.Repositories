using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Foundatio.Caching;
using Foundatio.Messaging;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Repositories.Elasticsearch.CustomFields;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Resilience;
using Foundatio.Serializer;
using Microsoft.Extensions.Logging;

namespace Foundatio.Repositories.Elasticsearch.Configuration;

/// <summary>
/// Provides configuration and lifecycle management for Elasticsearch indexes, including
/// client access, caching, messaging, serialization, and index maintenance operations.
/// </summary>
public interface IElasticConfiguration : IDisposable
{
    /// <summary>The configured Elasticsearch client used for all cluster operations.</summary>
    ElasticsearchClient Client { get; }

    /// <summary>Cache client used for repository-level caching and lock coordination.</summary>
    ICacheClient Cache { get; }

    /// <summary>Message bus used for entity change notifications and cache invalidation.</summary>
    IMessageBus MessageBus { get; }

    /// <summary>Serializer used for document serialization and deserialization.</summary>
    ITextSerializer Serializer { get; }

    /// <summary>Logger factory for creating loggers within the configuration and its indexes.</summary>
    ILoggerFactory LoggerFactory { get; }

    /// <summary>Provider for resilience policies (retry, circuit breaker) applied to Elasticsearch operations.</summary>
    IResiliencePolicyProvider ResiliencePolicyProvider { get; }

    /// <summary>Time provider used for time-dependent operations such as daily index naming.</summary>
    TimeProvider TimeProvider { get; set; }

    /// <summary>The collection of all registered index definitions.</summary>
    IReadOnlyCollection<IIndex> Indexes { get; }

    /// <summary>Repository for managing custom field definitions, or <c>null</c> if no custom field index is configured.</summary>
    ICustomFieldDefinitionRepository CustomFieldDefinitionRepository { get; }

    /// <summary>Returns the index with the specified <paramref name="name"/>, or <c>null</c> if not found.</summary>
    IIndex GetIndex(string name);

    /// <summary>Configures global query builders applied to every query across all indexes.</summary>
    void ConfigureGlobalQueryBuilders(ElasticQueryBuilder builder);

    /// <summary>Configures global query parser settings applied to every query across all indexes.</summary>
    void ConfigureGlobalQueryParsers(ElasticQueryParserConfiguration config);

    /// <summary>Creates and configures the specified indexes (or all registered indexes) in Elasticsearch.</summary>
    Task ConfigureIndexesAsync(IEnumerable<IIndex> indexes = null, bool beginReindexingOutdated = true);

    /// <summary>Runs maintenance tasks (e.g., alias management, cleanup) on the specified or all indexes.</summary>
    Task MaintainIndexesAsync(IEnumerable<IIndex> indexes = null);

    /// <summary>Deletes the specified indexes (or all registered indexes) from Elasticsearch.</summary>
    Task DeleteIndexesAsync(IEnumerable<IIndex> indexes = null);

    /// <summary>Reindexes outdated versioned indexes to their latest version.</summary>
    Task ReindexAsync(IEnumerable<IIndex> indexes = null, Func<int, string, Task> progressCallbackAsync = null);
}
