using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Serialization.Metadata;

namespace Foundatio.Repositories.Serialization;

/// <summary>
/// A <see cref="JsonTypeInfo"/> modifier that suppresses serialization of empty collections.
/// When applied via <c>JsonSerializerOptions.TypeInfoResolver</c>, any property whose
/// runtime value is an empty <see cref="ICollection"/> (including arrays, lists, dictionaries,
/// and sets) will be omitted from the JSON output.
/// </summary>
/// <remarks>
/// <para>
/// This replicates the Newtonsoft.Json behavior of <c>DefaultValueHandling.Ignore</c> for
/// collection-typed properties, reducing document noise in Elasticsearch and improving
/// round-trip fidelity during the STJ migration.
/// </para>
/// <para>
/// String properties are explicitly excluded even though <see cref="string"/> implements
/// <see cref="IEnumerable"/>; only types implementing <see cref="ICollection"/> are considered.
/// For properties typed as <see cref="IReadOnlyCollection{T}"/> where the runtime type does not
/// also implement <see cref="ICollection"/>, the modifier falls back to enumerating the first
/// element to determine emptiness.
/// </para>
/// </remarks>
public static class EmptyCollectionModifier
{
    /// <summary>
    /// Applies the empty-collection suppression modifier to the given <see cref="JsonTypeInfo"/>.
    /// Attach this to <see cref="DefaultJsonTypeInfoResolver.Modifiers"/> or use it with
    /// <c>JsonSerializerOptions.TypeInfoResolverChain</c>.
    /// </summary>
    public static void Modify(JsonTypeInfo typeInfo)
    {
        if (typeInfo.Kind != JsonTypeInfoKind.Object)
            return;

        foreach (var property in typeInfo.Properties)
        {
            if (property.PropertyType == typeof(string))
                continue;

            if (!typeof(ICollection).IsAssignableFrom(property.PropertyType)
                && !IsGenericCollectionInterface(property.PropertyType))
                continue;

            var existingPredicate = property.ShouldSerialize;
            property.ShouldSerialize = (obj, value) =>
            {
                if (existingPredicate is not null && !existingPredicate(obj, value))
                    return false;

                return IsNonEmptyCollection(value);
            };
        }
    }

    private static bool IsNonEmptyCollection(object? value)
    {
        if (value is null)
            return false;

        if (value is ICollection collection)
            return collection.Count > 0;

        if (value is IEnumerable enumerable)
            return enumerable.Cast<object>().Any();

        return true;
    }

    private static bool IsGenericCollectionInterface(Type type)
    {
        if (!type.IsGenericType)
            return false;

        var genericDef = type.GetGenericTypeDefinition();
        return genericDef == typeof(ICollection<>)
            || genericDef == typeof(IReadOnlyCollection<>);
    }
}
