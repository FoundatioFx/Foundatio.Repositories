using System;
using System.Globalization;
using Elastic.Clients.Elasticsearch.Mapping;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Indexes;

/// <summary>
/// Mirrors a daily index whose physical indexes are created and mapped by a system outside this
/// library (e.g. Logstash writing to <c>{name}-yyyy.MM.dd</c>). Regression fixture for
/// <see href="https://github.com/FoundatioFx/Foundatio.Repositories/issues/305">#305</see>: the
/// code-declared mapping (via <see cref="ConfigureIndexMapping"/>) always declares <c>id</c>, but
/// the real server-side mapping may have no <c>id</c> field at all, or a dynamically-mapped text
/// <c>id</c> field. Tests never write through this index -- indexes are created out-of-band by
/// posting documents directly to the client so Elasticsearch's dynamic mapping (not this
/// library's code mapping) determines the real server-side shape, exactly like an external writer.
/// </summary>
public sealed class ExternallyManagedLogEventIndex : DailyIndex<LogEvent>
{
    public ExternallyManagedLogEventIndex(IElasticConfiguration configuration)
        : base(configuration, "externally-managed-logevents", 1, doc => ((LogEvent)doc).Date.UtcDateTime)
    {
        HasSortableIdField = false;
    }

    public override void ConfigureIndexMapping(TypeMappingDescriptor<LogEvent> map)
    {
        map
            .Dynamic(DynamicMapping.False)
            .Properties(p => p
                .SetupDefaults()
                .Keyword(e => e.CompanyId)
                .Date(e => e.Date)
            );
    }

    // The externally-managed index has no version segment (e.g. "externally-managed-logevents-2026.08.03"),
    // so the default "{Name}-v{Version}-*" pattern this library uses for its own indexes never matches.
    protected override string MappingIndexPattern => $"{Name}-*";

    // The mapping filter selects candidates; this parser orders them and rejects malformed names.
    protected override DateTime GetIndexDate(string index)
    {
        if (DateTime.TryParseExact(index, $"'{Name}-'{DateFormat}", EnUs, DateTimeStyles.AdjustToUniversal, out var result))
            return DateTime.SpecifyKind(result.Date, DateTimeKind.Utc);

        return DateTime.MaxValue;
    }
}
