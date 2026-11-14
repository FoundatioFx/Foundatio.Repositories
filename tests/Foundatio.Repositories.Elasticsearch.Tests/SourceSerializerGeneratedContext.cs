using System.Collections.Generic;
using System.Text.Json.Serialization;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories.Elasticsearch.Tests;

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase, GenerationMode = JsonSourceGenerationMode.Metadata)]
[JsonSerializable(typeof(GeneratedSourceDocument))]
internal partial class SourceSerializerGeneratedContext : JsonSerializerContext
{
}

internal sealed class GeneratedSourceDocument : IVersioned
{
    public string? Version { get; set; }

    [JsonNumberHandling(JsonNumberHandling.AllowReadingFromString | JsonNumberHandling.WriteAsString)]
    public List<int> Values { get; set; } = [];
}
