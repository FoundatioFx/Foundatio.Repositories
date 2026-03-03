using System.Text.Json;
using System.Text.Json.Serialization;
using Foundatio.Repositories.Utility;

namespace Foundatio.Repositories.Extensions;

public static class JsonSerializerOptionsExtensions
{
    /// <summary>
    /// Configures <see cref="JsonSerializerOptions"/> with the defaults required for
    /// Foundatio.Repositories document serialization and round-tripping:
    /// <list type="bullet">
    ///   <item><see cref="JsonSerializerOptions.PropertyNameCaseInsensitive"/> set to <c>true</c> for case-insensitive property matching</item>
    ///   <item><see cref="JsonStringEnumConverter"/> with camelCase naming and integer fallback for enum values stored as strings in Elasticsearch</item>
    ///   <item><see cref="DoubleSystemTextJsonConverter"/> to preserve decimal points on whole-number doubles (workaround for dotnet/runtime#35195)</item>
    /// </list>
    /// </summary>
    /// <returns>The same <see cref="JsonSerializerOptions"/> instance for chaining.</returns>
    public static JsonSerializerOptions ConfigureFoundatioRepositoryDefaults(this JsonSerializerOptions options)
    {
        options.PropertyNameCaseInsensitive = true;
        options.Converters.Add(new JsonStringEnumConverter(JsonNamingPolicy.CamelCase, allowIntegerValues: true));
        options.Converters.Add(new DoubleSystemTextJsonConverter());
        return options;
    }
}
