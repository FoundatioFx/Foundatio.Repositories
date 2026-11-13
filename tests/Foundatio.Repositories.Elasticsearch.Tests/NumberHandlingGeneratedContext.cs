using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories.Elasticsearch.Tests;

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase, GenerationMode = JsonSourceGenerationMode.Metadata)]
[JsonSerializable(typeof(GeneratedNumberHandlingDocument))]
[JsonSerializable(typeof(GeneratedNumericNestedDocument))]
[JsonSerializable(typeof(int))]
[JsonSerializable(typeof(JsonElement))]
internal partial class NumberHandlingGeneratedContext : JsonSerializerContext
{
}

internal sealed class GeneratedNumberHandlingDocument : IVersioned
{
    public string? Version { get; set; } = "1:5";

    [JsonNumberHandling(JsonNumberHandling.AllowReadingFromString | JsonNumberHandling.WriteAsString)]
    public List<object> Values { get; set; } = [];

    public int Unannotated { get; set; } = 11;
}

internal sealed class GeneratedNumericNestedDocument : IVersioned
{
    public string? Version { get; set; }
    public int Number { get; set; }
}
