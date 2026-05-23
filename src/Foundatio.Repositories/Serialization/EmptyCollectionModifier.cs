using System;
using System.Collections;
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
/// <see cref="IEnumerable"/>. At registration time, any property whose type implements
/// <see cref="IEnumerable"/> (excluding <see cref="string"/>) is considered a collection.
/// The <c>ShouldSerialize</c> callback uses a runtime <see cref="ICollection"/> cast for the
/// count check, falling back to enumeration for types that only implement <see cref="IEnumerable"/>.
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

        var collectionProperties = typeInfo.Properties
            .Where(p => p.PropertyType != typeof(string))
            .Where(p => typeof(IEnumerable).IsAssignableFrom(p.PropertyType));

        foreach (var property in collectionProperties)
        {
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
        {
            var enumerator = enumerable.GetEnumerator();
            try
            {
                return enumerator.MoveNext();
            }
            finally
            {
                (enumerator as IDisposable)?.Dispose();
            }
        }

        return true;
    }
}
