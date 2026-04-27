using System.Linq;
using System.Text.Json;

namespace Foundatio.Repositories.Serialization;

public static class JsonSerializerOptionsExtensions
{
    /// <summary>
    /// Configures <see cref="JsonSerializerOptions"/> with the defaults required for
    /// Foundatio.Repositories document serialization and round-tripping:
    /// <list type="bullet">
    ///   <item><see cref="JsonSerializerOptions.PropertyNameCaseInsensitive"/> set to <c>true</c> for case-insensitive property matching</item>
    ///   <item><see cref="DoubleSystemTextJsonConverter"/> to preserve decimal points on whole-number doubles (workaround for dotnet/runtime#35195)</item>
    ///   <item><see cref="ObjectToInferredTypesConverter"/> to deserialize <see cref="object"/>-typed properties as CLR primitives instead of <see cref="System.Text.Json.JsonElement"/></item>
    /// </list>
    /// Enums serialize as integers by default (matching the prior NEST/Newtonsoft default).
    /// </summary>
    /// <param name="options">The options instance to configure.</param>
    /// <returns>The same <see cref="JsonSerializerOptions"/> instance for chaining.</returns>
    public static JsonSerializerOptions ConfigureFoundatioRepositoryDefaults(this JsonSerializerOptions options)
    {
        options.PropertyNameCaseInsensitive = true;

        if (!options.Converters.Any(c => c is DoubleSystemTextJsonConverter))
            options.Converters.Add(new DoubleSystemTextJsonConverter());

        if (!options.Converters.Any(c => c is ObjectToInferredTypesConverter))
            options.Converters.Add(new ObjectToInferredTypesConverter());

        return options;
    }
}
