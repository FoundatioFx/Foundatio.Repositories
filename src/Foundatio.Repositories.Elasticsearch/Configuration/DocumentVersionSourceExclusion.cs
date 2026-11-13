using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using System.Threading;
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
/// types are never changed, which lets models store a business version on a child object. This also holds for values
/// reached through <see cref="object"/> properties and collections such as <c>Dictionary&lt;string, object&gt;</c>,
/// where the document type is only known at runtime.</para>
/// <para>With a source-generated <c>JsonSerializerContext</c> the property metadata carries no reflection information, so
/// the implementing property's configured serialized name is matched instead.</para>
/// <para>Excluding the property also means a <c>version</c> value already stored in an older document's <c>_source</c> is
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

    /// <summary>
    /// <c>true</c> when <paramref name="type"/> is, contains, or may hold a root type through polymorphism or collection
    /// elements whose runtime types are not available from the declared property type.
    /// </summary>
    internal static bool ContainsRootDocumentType(Type type, Func<Type, bool> isRegistered)
    {
        if (type == typeof(string))
            return false;

        if (IsRootDocumentType(type, isRegistered) || type.IsInterface || !type.IsSealed || typeof(IEnumerable).IsAssignableFrom(type))
            return true;

        return type.IsGenericType && type.GetGenericArguments().Any(a => ContainsRootDocumentType(a, isRegistered));
    }

    internal static PropertyInfo? GetDocumentVersionProperty(Type type)
    {
        if (!typeof(IVersioned).IsAssignableFrom(type))
            return null;

        var versionProperty = typeof(IVersioned).GetProperty(nameof(IVersioned.Version))!;
        if (type.IsInterface)
            return versionProperty;

        var map = type.GetInterfaceMap(typeof(IVersioned));
        var getter = map.TargetMethods[Array.IndexOf(map.InterfaceMethods, versionProperty.GetMethod!)];
        return getter.DeclaringType!.GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
            .FirstOrDefault(property => IsSameMethod(property.GetMethod, getter));
    }

    private static bool IsSameMethod(MethodInfo? first, MethodInfo? second)
    {
        return first is not null && second is not null && first.Module == second.Module && first.MetadataToken == second.MetadataToken;
    }

    private sealed class RootVersionExcludingResolver : IJsonTypeInfoResolver
    {
        private readonly IJsonTypeInfoResolver _inner;
        private readonly Func<Type, bool> _isRegistered;
        private volatile JsonSerializerOptions? _nestedOptions;

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
            var versionProperty = isRoot ? GetDocumentVersionProperty(type) : null;
            for (int index = typeInfo.Properties.Count - 1; index >= 0; index--)
            {
                var property = typeInfo.Properties[index];
                if (versionProperty is not null && IsDocumentVersionProperty(property, versionProperty, options))
                {
                    // Keep the name recognized so a legacy value isn't captured by extension data.
                    property.Get = null;
                    property.Set = null;
                    property.IsRequired = false;
                    continue;
                }

                if (property.IsExtensionData)
                {
                    if (property.CustomConverter is null && typeof(IDictionary<string, object>).IsAssignableFrom(property.PropertyType))
                        property.CustomConverter = (JsonConverter)Activator.CreateInstance(typeof(NestedExtensionDataConverter<>).MakeGenericType(property.PropertyType), GetNestedOptions(options))!;

                    continue;
                }

                if (property.CustomConverter is null && ContainsRootDocumentType(property.PropertyType, _isRegistered))
                    property.CustomConverter = CreateNestedConverter(property.PropertyType, options);
            }

            return typeInfo;
        }

        /// <summary>
        /// Reflection-based resolvers expose the <see cref="PropertyInfo"/>. Source-generated resolvers don't, so the
        /// implementing property's configured serialized name is matched instead.
        /// </summary>
        private static bool IsDocumentVersionProperty(JsonPropertyInfo property, PropertyInfo versionProperty, JsonSerializerOptions options)
        {
            if (property.AttributeProvider is PropertyInfo info)
                return IsSameMethod(info.GetMethod, versionProperty.GetMethod);

            string name = versionProperty.GetCustomAttribute<JsonPropertyNameAttribute>()?.Name
                ?? options.PropertyNamingPolicy?.ConvertName(versionProperty.Name)
                ?? versionProperty.Name;
            return property.PropertyType == typeof(string) && String.Equals(property.Name, name, StringComparison.Ordinal);
        }

        /// <summary>Serializes a nested value with the original resolver, so root types inside it keep their version.</summary>
        private JsonConverter CreateNestedConverter(Type propertyType, JsonSerializerOptions options)
        {
            var nestedOptions = GetNestedOptions(options);
            return (JsonConverter)Activator.CreateInstance(typeof(NestedValueConverter<>).MakeGenericType(propertyType), nestedOptions)!;
        }

        private JsonSerializerOptions GetNestedOptions(JsonSerializerOptions options)
        {
            return _nestedOptions ?? Interlocked.CompareExchange(ref _nestedOptions, new JsonSerializerOptions(options) { TypeInfoResolver = _inner }, null) ?? _nestedOptions!;
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

    private sealed class NestedExtensionDataConverter<TDictionary>(JsonSerializerOptions nestedOptions) : JsonConverter<TDictionary>
        where TDictionary : IDictionary<string, object>
    {
        public override TDictionary? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            return JsonSerializer.Deserialize<TDictionary>(ref reader, nestedOptions);
        }

        public override void Write(Utf8JsonWriter writer, TDictionary value, JsonSerializerOptions options)
        {
            // Extension data is already inside the containing object, so only its properties are written.
            foreach (var entry in value)
            {
                writer.WritePropertyName(entry.Key);
                JsonSerializer.Serialize(writer, entry.Value, nestedOptions);
            }
        }
    }
}
