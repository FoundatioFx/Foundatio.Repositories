using System;
using System.Collections;
using System.Linq;
using System.Text.Json.Serialization.Metadata;

namespace Foundatio.Repositories.Serialization;

/// <summary>
/// A <see cref="JsonTypeInfo"/> modifier that suppresses serialization of empty or null collections.
/// When applied via <c>JsonSerializerOptions.TypeInfoResolver</c>, any property whose declared
/// type implements <see cref="IEnumerable"/> (excluding <see cref="string"/>) will be omitted
/// from the JSON output when the runtime value is <c>null</c> or contains zero elements.
/// </summary>
/// <remarks>
/// <para>
/// This replicates the Newtonsoft.Json behavior of <c>DefaultValueHandling.Ignore</c> for
/// collection-typed properties, reducing document noise in Elasticsearch and improving
/// round-trip fidelity during the STJ migration.
/// </para>
/// <para>
/// At registration time, any property whose type implements <see cref="IEnumerable"/>
/// (excluding <see cref="string"/>) is considered a collection. At serialization time:
/// </para>
/// <list type="number">
///   <item>If the value is <c>null</c>, the property is omitted.</item>
///   <item>If the value implements <see cref="ICollection"/>, the <c>Count</c> property is checked (O(1)).</item>
///   <item>Otherwise, <c>GetEnumerator().MoveNext()</c> probes for emptiness (one element advance).</item>
/// </list>
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
