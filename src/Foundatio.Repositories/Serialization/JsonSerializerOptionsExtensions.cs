using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Foundatio.Repositories.Serialization;

public static class JsonSerializerOptionsExtensions
{
    /// <summary>
    /// Configures <see cref="JsonSerializerOptions"/> with the defaults required for
    /// Foundatio.Repositories document serialization and round-tripping:
    /// <list type="bullet">
    ///   <item><see cref="JsonSerializerOptions.PropertyNameCaseInsensitive"/> set to <c>true</c> for case-insensitive property matching</item>
    ///   <item><see cref="SafeJsonEncoder"/> for non-ASCII passthrough with HTML character escaping</item>
    ///   <item><see cref="DoubleSystemTextJsonConverter"/> to preserve decimal points on whole-number doubles (workaround for dotnet/runtime#35195)</item>
    ///   <item><see cref="ObjectToInferredTypesConverter"/> to deserialize <see cref="object"/>-typed properties as CLR primitives instead of <see cref="System.Text.Json.JsonElement"/></item>
    /// </list>
    /// Enums serialize as integers by default (matching the prior NEST/Newtonsoft default).
    /// </summary>
    /// <remarks>
    /// <para><b>Nullable annotations:</b> Consider setting
    /// <c>options.RespectNullableAnnotations = true</c> on .NET 9+ to reject <c>null</c>
    /// values for non-nullable reference type properties during deserialization. This catches
    /// data integrity issues early but requires all model properties that can legitimately be
    /// null to be declared nullable. This is <b>not</b> enabled by default because it is a
    /// breaking behavior change for existing models.</para>
    /// <para><b>Null serialization with Elastic.Clients.Elasticsearch 8.19.22+:</b> The ES client
    /// sets <c>DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull</c> on its internal
    /// serializer options. If you share options with the ES client, be aware that null properties
    /// will be omitted from request bodies. For document serialization where null-to-remove
    /// semantics are needed (e.g., patch operations), use a separate <see cref="JsonSerializerOptions"/>
    /// instance without this setting.</para>
    /// </remarks>
    /// <param name="options">The options instance to configure.</param>
    /// <returns>The same <see cref="JsonSerializerOptions"/> instance for chaining.</returns>
    public static JsonSerializerOptions ConfigureFoundatioRepositoryDefaults(this JsonSerializerOptions options)
    {
        options.PropertyNameCaseInsensitive = true;
        options.Encoder = SafeJsonEncoder.Instance;

        if (!options.Converters.Any(c => c is DoubleSystemTextJsonConverter))
            options.Converters.Add(new DoubleSystemTextJsonConverter());

        if (!options.Converters.Any(c => c is ObjectToInferredTypesConverter))
            options.Converters.Add(new ObjectToInferredTypesConverter());

        return options;
    }

    /// <summary>
    /// Configures <see cref="JsonSerializerOptions"/> with the repository defaults and additionally
    /// applies the <see cref="EmptyCollectionModifier"/> to suppress serialization of empty collections.
    /// </summary>
    /// <param name="options">The options instance to configure.</param>
    /// <returns>The same <see cref="JsonSerializerOptions"/> instance for chaining.</returns>
    public static JsonSerializerOptions ConfigureFoundatioRepositoryDefaultsWithModifiers(this JsonSerializerOptions options)
    {
        options.ConfigureFoundatioRepositoryDefaults();

        var resolver = new DefaultJsonTypeInfoResolver
        {
            Modifiers = { EmptyCollectionModifier.Modify }
        };
        options.TypeInfoResolverChain.Insert(0, resolver);

        return options;
    }
}
