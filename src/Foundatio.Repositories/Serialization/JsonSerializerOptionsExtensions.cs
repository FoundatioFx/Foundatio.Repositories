using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;

namespace Foundatio.Repositories.Serialization;

public static class JsonSerializerOptionsExtensions
{
    /// <summary>
    /// Configures <see cref="JsonSerializerOptions"/> with the defaults required for
    /// Foundatio.Repositories document serialization and round-tripping:
    /// <list type="bullet">
    ///   <item><see cref="JsonSerializerOptions.PropertyNameCaseInsensitive"/> set to <c>true</c> for case-insensitive property matching</item>
    ///   <item><see cref="JsonSerializerOptions.DefaultIgnoreCondition"/> set to <see cref="JsonIgnoreCondition.Never"/> to preserve null values in partial updates</item>
    ///   <item><see cref="SafeJsonEncoder"/> for non-ASCII passthrough with HTML character escaping</item>
    ///   <item><see cref="DoubleSystemTextJsonConverter"/> to preserve decimal points on whole-number doubles (workaround for dotnet/runtime#35195)</item>
    ///   <item><see cref="ObjectToInferredTypesConverter"/> to deserialize <see cref="object"/>-typed properties as CLR primitives instead of <see cref="System.Text.Json.JsonElement"/></item>
    /// </list>
    /// Enums serialize as integers by default (matching the prior NEST/Newtonsoft default).
    /// </summary>
    /// <remarks>
    /// <para><b>Null serialization:</b> <c>DefaultIgnoreCondition</c> is explicitly set to
    /// <see cref="JsonIgnoreCondition.Never"/> so that partial document updates (e.g.,
    /// <c>new { companyName = (string?)null }</c>) correctly serialize the null value and
    /// clear the field in Elasticsearch. This is safe with Elastic.Clients.Elasticsearch 8.19.22+
    /// because the client now uses the <c>RequestResponseSerializer</c> for internal multi-variant
    /// union types (elastic/elasticsearch-net#8763).</para>
    /// <para><b>Nullable annotations:</b> Consider setting
    /// <c>options.RespectNullableAnnotations = true</c> on .NET 9+ to reject <c>null</c>
    /// values for non-nullable reference type properties during deserialization. This catches
    /// data integrity issues early but requires all model properties that can legitimately be
    /// null to be declared nullable. This is <b>not</b> enabled by default because it is a
    /// breaking behavior change for existing models.</para>
    /// </remarks>
    /// <param name="options">The options instance to configure.</param>
    /// <returns>The same <see cref="JsonSerializerOptions"/> instance for chaining.</returns>
    public static JsonSerializerOptions ConfigureFoundatioRepositoryDefaults(this JsonSerializerOptions options)
    {
        options.PropertyNameCaseInsensitive = true;
        options.DefaultIgnoreCondition = JsonIgnoreCondition.Never;
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
