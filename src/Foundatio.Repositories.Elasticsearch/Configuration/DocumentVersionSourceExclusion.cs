using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories.Elasticsearch.Configuration;

/// <summary>
/// Keeps <see cref="IVersioned.Version"/> out of the Elasticsearch <c>_source</c>. The version is concurrency metadata
/// (<c>_seq_no</c> and <c>_primary_term</c>) that Elasticsearch assigns after the document is serialized, so a stored copy
/// is always stale. Every read populates <see cref="IVersioned.Version"/> from hit metadata instead.
/// </summary>
/// <remarks>
/// <para>The property is removed only from root document types: the <c>T</c> of every <see cref="IIndex{T}"/> in a
/// configuration, and types derived from them (a derived document stored through a base repository). The set of types is
/// scoped to the serializer options it is applied to, so two configurations never affect each other.</para>
/// <para>Only the document itself loses the property. Wherever a root document type appears nested inside another value
/// (for example an <c>Employee</c> embedded in another document, or in a list), it is serialized with the unmodified
/// options and keeps its <c>version</c>. Nested objects that implement <see cref="IVersioned"/> but are not root document
/// types are never changed, which lets models store a business version on a child object.</para>
/// <para>Removing the property also means a <c>version</c> value already stored in an older document's <c>_source</c> is
/// ignored when it is read.</para>
/// <para><see cref="ElasticConfiguration"/> applies this to its source serializer automatically. If you replace the
/// source serializer, call <see cref="ExcludeDocumentVersionFromSource(JsonSerializerOptions, IEnumerable{Type})"/> on
/// its options. Never apply it to the cache serializer: cached documents must keep their version so they can still be
/// saved with an optimistic concurrency check.</para>
/// </remarks>
public static class DocumentVersionSourceExclusion
{
    /// <summary>
    /// Wraps the options' type info resolver so <see cref="IVersioned.Version"/> is excluded when one of
    /// <paramref name="documentTypes"/> (or a type derived from one) is the serialized document. Types that don't
    /// implement <see cref="IVersioned"/> are ignored. Call before the options are first used.
    /// </summary>
    public static JsonSerializerOptions ExcludeDocumentVersionFromSource(this JsonSerializerOptions options, IEnumerable<Type> documentTypes)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(documentTypes);

        var roots = documentTypes.Where(t => typeof(IVersioned).IsAssignableFrom(t)).ToHashSet();
        return roots.Count is 0 ? options : options.ExcludeDocumentVersionFromSource(roots.Contains);
    }

    /// <summary>
    /// Same as <see cref="ExcludeDocumentVersionFromSource(JsonSerializerOptions, IEnumerable{Type})"/>, with a live
    /// registry: <paramref name="isRegisteredDocumentType"/> is evaluated when a type is first serialized.
    /// </summary>
    internal static JsonSerializerOptions ExcludeDocumentVersionFromSource(this JsonSerializerOptions options, Func<Type, bool> isRegisteredDocumentType)
    {
        options.TypeInfoResolver = new RootVersionExcludingResolver(options.TypeInfoResolver ?? new DefaultJsonTypeInfoResolver(), isRegisteredDocumentType);
        return options;
    }

    /// <summary><c>true</c> when <paramref name="type"/> is a registered document type or derives from one.</summary>
    internal static bool IsRootDocumentType(Type type, Func<Type, bool> isRegistered)
    {
        for (var current = type; current is not null && current != typeof(object); current = current.BaseType)
        {
            if (isRegistered(current))
                return true;
        }

        return false;
    }

    /// <summary><c>true</c> when <paramref name="type"/> is, or contains (array element or generic argument), a root type.</summary>
    internal static bool ContainsRootDocumentType(Type type, Func<Type, bool> isRegistered)
    {
        if (IsRootDocumentType(type, isRegistered))
            return true;

        if (type.IsArray)
            return ContainsRootDocumentType(type.GetElementType()!, isRegistered);

        return type.IsGenericType && type.GetGenericArguments().Any(a => ContainsRootDocumentType(a, isRegistered));
    }

    private sealed class RootVersionExcludingResolver : IJsonTypeInfoResolver
    {
        private readonly IJsonTypeInfoResolver _inner;
        private readonly Func<Type, bool> _isRegistered;
        private JsonSerializerOptions? _nestedOptions;

        public RootVersionExcludingResolver(IJsonTypeInfoResolver inner, Func<Type, bool> isRegistered)
        {
            _inner = inner;
            _isRegistered = isRegistered;
        }

        public JsonTypeInfo? GetTypeInfo(Type type, JsonSerializerOptions options)
        {
            var typeInfo = _inner.GetTypeInfo(type, options);
            if (typeInfo is not { Kind: JsonTypeInfoKind.Object })
                return typeInfo;

            bool isRoot = IsRootDocumentType(type, _isRegistered);
            for (int index = typeInfo.Properties.Count - 1; index >= 0; index--)
            {
                var property = typeInfo.Properties[index];
                if (isRoot && property.AttributeProvider is PropertyInfo { Name: nameof(IVersioned.Version) } info && info.PropertyType == typeof(string))
                {
                    typeInfo.Properties.RemoveAt(index);
                    continue;
                }

                if (property.CustomConverter is null && ContainsRootDocumentType(property.PropertyType, _isRegistered))
                    property.CustomConverter = CreateNestedConverter(property.PropertyType, options);
            }

            return typeInfo;
        }

        /// <summary>Serializes a nested value with the original resolver, so root types inside it keep their version.</summary>
        private JsonConverter CreateNestedConverter(Type propertyType, JsonSerializerOptions options)
        {
            _nestedOptions ??= new JsonSerializerOptions(options) { TypeInfoResolver = _inner };
            return (JsonConverter)Activator.CreateInstance(typeof(NestedValueConverter<>).MakeGenericType(propertyType), _nestedOptions)!;
        }
    }

    private sealed class NestedValueConverter<TValue>(JsonSerializerOptions nestedOptions) : JsonConverter<TValue>
    {
        public override bool HandleNull => true;

        public override TValue? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            return JsonSerializer.Deserialize<TValue>(ref reader, nestedOptions);
        }

        public override void Write(Utf8JsonWriter writer, TValue value, JsonSerializerOptions options)
        {
            JsonSerializer.Serialize(writer, value, nestedOptions);
        }
    }
}
