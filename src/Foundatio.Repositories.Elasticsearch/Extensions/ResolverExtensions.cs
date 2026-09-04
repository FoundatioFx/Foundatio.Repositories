using System;
using System.Collections.Generic;
using System.Linq;
using Elastic.Clients.Elasticsearch;
using Foundatio.Parsers.ElasticQueries;

namespace Foundatio.Repositories.Elasticsearch.Extensions;

public static class ResolverExtensions
{
    public static ICollection<Field> GetResolvedFields(this ElasticMappingResolver resolver, ICollection<Field> fields)
    {
        if (fields.Count == 0)
            return fields;

        return fields.Select(field => ResolveFieldName(resolver, field)).ToList();
    }

    public static ICollection<SortOptions> GetResolvedFields(this ElasticMappingResolver resolver, ICollection<SortOptions> sorts)
    {
        if (sorts.Count == 0)
            return sorts;

        return sorts.Select(sort => ResolveFieldSort(resolver, sort)).OfType<SortOptions>().ToList();
    }

    public static Field ResolveFieldName(this ElasticMappingResolver resolver, Field field)
    {
        if (field is null)
            throw new ArgumentNullException(nameof(field));

        return new Field(resolver.GetResolvedField(field), field.Boost);
    }

    public static SortOptions? ResolveFieldSort(this ElasticMappingResolver resolver, SortOptions? sort)
    {
        if (sort?.Field is { } fieldSort)
        {
            var resolvedField = resolver.GetSortFieldName(fieldSort.Field);
            return new FieldSort
            {
                Field = resolvedField,
                Format = fieldSort.Format,
                Missing = fieldSort.Missing,
                Mode = fieldSort.Mode,
                Nested = fieldSort.Nested,
                NumericType = fieldSort.NumericType,
                Order = fieldSort.Order,
                UnmappedType = fieldSort.UnmappedType
            };
        }

        if (sort?.Score is { } scoreSort)
            return new SortOptions { Score = new ScoreSort { Order = scoreSort.Order } };

        if (sort?.Doc is { } docSort)
            return new SortOptions { Doc = new ScoreSort { Order = docSort.Order } };

        if (sort?.GeoDistance is { } geoSort)
        {
            return new GeoDistanceSort
            {
                Field = geoSort.Field,
                Location = geoSort.Location,
                DistanceType = geoSort.DistanceType,
                IgnoreUnmapped = geoSort.IgnoreUnmapped,
                Mode = geoSort.Mode,
                Nested = geoSort.Nested,
                Order = geoSort.Order,
                Unit = geoSort.Unit
            };
        }

        if (sort?.Script is { } scriptSort)
        {
            return new ScriptSort
            {
                Script = scriptSort.Script,
                Type = scriptSort.Type,
                Mode = scriptSort.Mode,
                Nested = scriptSort.Nested,
                Order = scriptSort.Order
            };
        }

        return sort;
    }
}
