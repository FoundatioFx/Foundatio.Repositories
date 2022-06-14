using System;
using System.Collections.Generic;
using System.Linq;
using Foundatio.Parsers.ElasticQueries;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Extensions;

public static class ResolverExtensions {
    public static ICollection<Field> GetResolvedFields(this ElasticMappingResolver resolver, ICollection<Field> fields) {
        if (fields.Count == 0)
            return fields;

        return fields.Select(field => ResolveFieldName(resolver, field)).ToList();
    }

    public static ICollection<IFieldSort> GetResolvedFields(this ElasticMappingResolver resolver, ICollection<IFieldSort> sorts) {
        if (sorts.Count == 0)
            return sorts;

        return sorts.Select(sort => ResolveFieldSort(resolver, sort)).ToList();
    }

    public static Field ResolveFieldName(this ElasticMappingResolver resolver, Field field) {
        if (field?.Name == null)
            return field;

        return new Field(resolver.GetResolvedField(field.Name), field.Boost, field.Format);
    }

    public static IFieldSort ResolveFieldSort(this ElasticMappingResolver resolver, IFieldSort sort) {
        return new FieldSort {
            Field = resolver.GetSortFieldName(sort.SortKey),
            IgnoreUnmappedFields = sort.IgnoreUnmappedFields,
            Missing = sort.Missing,
            Mode = sort.Mode,
            Nested = sort.Nested,
            NumericType = sort.NumericType,
            Order = sort.Order,
            UnmappedType = sort.UnmappedType
        };
    }
}
