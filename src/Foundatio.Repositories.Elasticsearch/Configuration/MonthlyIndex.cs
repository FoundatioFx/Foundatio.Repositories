﻿using System;
using System.Collections.Generic;
using Exceptionless.DateTimeExtensions;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Configuration;

public class MonthlyIndex : DailyIndex
{
    public MonthlyIndex(IElasticConfiguration configuration, string name, int version = 1, Func<object, DateTime> getDocumentDateUtc = null)
        : base(configuration, name, version, getDocumentDateUtc)
    {
        DateFormat = "yyyy.MM";
    }

    public override string[] GetIndexes(DateTime? utcStart, DateTime? utcEnd)
    {
        if (!utcStart.HasValue)
            utcStart = Configuration.TimeProvider.GetUtcNow().UtcDateTime;

        if (!utcEnd.HasValue || utcEnd.Value < utcStart)
            utcEnd = Configuration.TimeProvider.GetUtcNow().UtcDateTime;

        var utcStartOfDay = utcStart.Value.StartOfDay();
        var utcEndOfDay = utcEnd.Value.EndOfDay();
        var period = utcEndOfDay - utcStartOfDay;
        if ((MaxIndexAge.HasValue && period > MaxIndexAge.Value) || period.GetTotalYears() > 1)
            return Array.Empty<string>();

        var utcEndOfMonth = utcEnd.Value.EndOfMonth();

        var indices = new List<string>();
        for (var current = utcStartOfDay; current <= utcEndOfMonth; current = current.AddMonths(1))
            indices.Add(GetIndexByDate(current));

        return indices.ToArray();
    }

    protected override DateTime GetIndexExpirationDate(DateTime utcDate)
    {
        return MaxIndexAge.HasValue && MaxIndexAge > TimeSpan.Zero ? utcDate.EndOfMonth().SafeAdd(MaxIndexAge.Value) : DateTime.MaxValue;
    }

    protected override bool ShouldCreateAlias(DateTime documentDateUtc, IndexAliasAge alias)
    {
        if (alias.MaxAge == TimeSpan.MaxValue)
            return true;

        return Configuration.TimeProvider.GetUtcNow().UtcDateTime.Date.SafeSubtract(alias.MaxAge) <= documentDateUtc.EndOfMonth();
    }
}

public class MonthlyIndex<T> : MonthlyIndex where T : class
{
    private readonly string _typeName = typeof(T).Name.ToLower();

    public MonthlyIndex(IElasticConfiguration configuration, string name = null, int version = 1, Func<object, DateTime> getDocumentDateUtc = null) : base(configuration, name, version, getDocumentDateUtc)
    {
        Name = name ?? _typeName;
    }

    protected override ElasticMappingResolver CreateMappingResolver()
    {
        return ElasticMappingResolver.Create<T>(ConfigureIndexMapping, Configuration.Client.Infer, GetLatestIndexMapping, _logger);
    }

    public virtual TypeMappingDescriptor<T> ConfigureIndexMapping(TypeMappingDescriptor<T> map)
    {
        return map.AutoMap<T>().Properties(p => p.SetupDefaults());
    }

    public override CreateIndexDescriptor ConfigureIndex(CreateIndexDescriptor idx)
    {
        idx = base.ConfigureIndex(idx);
        return idx.Map<T>(f =>
        {
            if (CustomFieldTypes.Count > 0)
            {
                f.DynamicTemplates(d =>
                {
                    foreach (var customFieldType in CustomFieldTypes.Values)
                        d.DynamicTemplate($"idx_{customFieldType.Type}", df => df.PathMatch("idx.*").Match($"{customFieldType.Type}-*").Mapping(customFieldType.ConfigureMapping));

                    return d;
                });
            }

            return ConfigureIndexMapping(f);
        });
    }

    public override void ConfigureSettings(ConnectionSettings settings)
    {
        settings.DefaultMappingFor<T>(d => d.IndexName(Name));
    }
}
