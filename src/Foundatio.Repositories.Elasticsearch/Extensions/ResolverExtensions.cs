using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Repositories.Extensions;

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

    /// <summary>Resolves field names asynchronously while preserving their boosts and input order.</summary>
    public static async ValueTask<ICollection<Field>> GetResolvedFieldsAsync(this ElasticMappingResolver resolver,
        ICollection<Field> fields, CancellationToken cancellationToken = default)
    {
        if (fields.Count == 0)
            return fields;

        var resolved = new List<Field>(fields.Count);
        foreach (var field in fields)
            resolved.Add(await resolver.ResolveFieldNameAsync(field, cancellationToken).AnyContext());
        return resolved;
    }

    /// <summary>Resolves field sorts asynchronously while preserving sort settings and non-field sort variants.</summary>
    public static async ValueTask<ICollection<SortOptions>> GetResolvedFieldsAsync(this ElasticMappingResolver resolver,
        ICollection<SortOptions> sorts, CancellationToken cancellationToken = default)
    {
        if (sorts.Count == 0)
            return sorts;

        var resolved = new List<SortOptions>(sorts.Count);
        foreach (var sort in sorts)
        {
            var resolvedSort = await resolver.ResolveFieldSortAsync(sort, cancellationToken).AnyContext();
            if (resolvedSort is not null)
                resolved.Add(resolvedSort);
        }
        return resolved;
    }

    /// <summary>Resolves a field name asynchronously, retaining its boost.</summary>
    public static async ValueTask<Field> ResolveFieldNameAsync(this ElasticMappingResolver resolver, Field field,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(field);
        return new Field(await resolver.GetResolvedFieldAsync(field, cancellationToken).AnyContext(), field.Boost);
    }

    /// <summary>Resolves a field sort asynchronously, retaining its settings, or returns a non-field sort unchanged.</summary>
    public static async ValueTask<SortOptions?> ResolveFieldSortAsync(this ElasticMappingResolver resolver, SortOptions? sort,
        CancellationToken cancellationToken = default)
    {
        if (sort?.Field is not { } fieldSort)
            return sort;

        var resolvedField = await resolver.GetSortFieldNameAsync(fieldSort.Field, cancellationToken).AnyContext();
        return CreateFieldSort(fieldSort, resolvedField);
    }

    public static Field ResolveFieldName(this ElasticMappingResolver resolver, Field field)
    {
        if (field is null)
            throw new ArgumentNullException(nameof(field));

        return new Field(resolver.GetResolvedField(field), field.Boost);
    }

    public static SortOptions? ResolveFieldSort(this ElasticMappingResolver resolver, SortOptions? sort)
    {
        // SortOptions is a discriminated union - check if it's a field sort
        if (sort?.Field != null)
        {
            var fieldSort = sort.Field;
            var resolvedField = resolver.GetSortFieldName(fieldSort.Field);
            return CreateFieldSort(fieldSort, resolvedField);
        }

        return sort;
    }

    private static FieldSort CreateFieldSort(FieldSort fieldSort, string resolvedField) => new()
    {
        Field = resolvedField,
        Missing = fieldSort.Missing,
        Mode = fieldSort.Mode,
        Nested = fieldSort.Nested,
        NumericType = fieldSort.NumericType,
        Order = fieldSort.Order,
        UnmappedType = fieldSort.UnmappedType
    };
}
