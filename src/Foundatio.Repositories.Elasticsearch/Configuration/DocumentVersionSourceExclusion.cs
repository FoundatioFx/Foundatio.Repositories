using System;
using System.Collections.Concurrent;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories.Elasticsearch.Configuration;

/// <summary>
/// Keeps <see cref="IVersioned.Version"/> out of the Elasticsearch <c>_source</c>. The version is concurrency metadata
/// (<c>_seq_no</c> and <c>_primary_term</c>) that Elasticsearch assigns after the document is serialized, so a stored copy
/// is always stale. Every read populates <see cref="IVersioned.Version"/> from hit metadata instead.
/// </summary>
/// <remarks>
/// <para>The property is removed only from registered root document types (the <c>T</c> of every repository and
/// <see cref="IIndex{T}"/>). Nested objects that implement <see cref="IVersioned"/> keep their <c>version</c> field, which
/// lets models store their own business version on a child object.</para>
/// <para>Removing the property also means a <c>version</c> value already stored in an older document's <c>_source</c> is
/// ignored when it is read.</para>
/// <para><see cref="ElasticConfiguration"/> applies this to its source serializer automatically. If you replace the
/// source serializer, call <see cref="ExcludeDocumentVersionFromSource(JsonSerializerOptions)"/> on its options.</para>
/// <para>Apply this only to the Elasticsearch source serializer. The cache serializer must keep the version so cached
/// documents can still be saved with an optimistic concurrency check.</para>
/// </remarks>
public static class DocumentVersionSourceExclusion
{
    private static readonly ConcurrentDictionary<Type, byte> _documentTypes = new();

    /// <summary>
    /// Registers a root document type whose <see cref="IVersioned.Version"/> property is excluded from <c>_source</c>.
    /// Types that do not implement <see cref="IVersioned"/> are ignored. Register before the type is first serialized.
    /// </summary>
    public static void RegisterDocumentType(Type documentType)
    {
        ArgumentNullException.ThrowIfNull(documentType);

        if (typeof(IVersioned).IsAssignableFrom(documentType))
            _documentTypes.TryAdd(documentType, 0);
    }

    /// <summary>
    /// Returns <c>true</c> when <paramref name="documentType"/> is a registered root document type.
    /// </summary>
    public static bool IsDocumentType(Type documentType)
    {
        ArgumentNullException.ThrowIfNull(documentType);
        return _documentTypes.ContainsKey(documentType);
    }

    /// <summary>
    /// A <see cref="JsonTypeInfo"/> modifier that removes the <see cref="IVersioned.Version"/> property from registered
    /// root document types.
    /// </summary>
    public static void Modify(JsonTypeInfo typeInfo)
    {
        ArgumentNullException.ThrowIfNull(typeInfo);

        if (typeInfo.Kind is not JsonTypeInfoKind.Object || !IsDocumentType(typeInfo.Type))
            return;

        for (int index = typeInfo.Properties.Count - 1; index >= 0; index--)
        {
            if (typeInfo.Properties[index].AttributeProvider is PropertyInfo { Name: nameof(IVersioned.Version) } property
                && property.PropertyType == typeof(string))
            {
                typeInfo.Properties.RemoveAt(index);
            }
        }
    }

    /// <summary>
    /// Wraps the options' type info resolver so <see cref="IVersioned.Version"/> is excluded from registered root document types.
    /// </summary>
    public static JsonSerializerOptions ExcludeDocumentVersionFromSource(this JsonSerializerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (options.TypeInfoResolver is ModifyingTypeInfoResolver)
            return options;

        options.TypeInfoResolver = new ModifyingTypeInfoResolver(options.TypeInfoResolver ?? new DefaultJsonTypeInfoResolver(), Modify);
        return options;
    }

    private sealed class ModifyingTypeInfoResolver(IJsonTypeInfoResolver inner, Action<JsonTypeInfo> modifier) : IJsonTypeInfoResolver
    {
        public JsonTypeInfo? GetTypeInfo(Type type, JsonSerializerOptions options)
        {
            var typeInfo = inner.GetTypeInfo(type, options);
            if (typeInfo is not null)
                modifier(typeInfo);

            return typeInfo;
        }
    }
}
