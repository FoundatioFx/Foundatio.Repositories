using System;
using System.Text.Json;
using System.Text.Json.Serialization;
using Elastic.Clients.Elasticsearch.Core.Search;
using ILazyDocument = Foundatio.Repositories.Models.ILazyDocument;

namespace Foundatio.Repositories.Elasticsearch.Extensions;

public class ElasticLazyDocument : ILazyDocument
{
    private readonly Hit<object> _hit;
    private static readonly JsonSerializerOptions _jsonSerializerOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        Converters = { new JsonStringEnumConverter(JsonNamingPolicy.CamelCase, allowIntegerValues: true) }
    };

    public ElasticLazyDocument(Hit<object> hit)
    {
        _hit = hit;
    }

    public T As<T>() where T : class
    {
        if (_hit?.Source == null)
            return null;

        if (_hit.Source is T typed)
            return typed;

        if (_hit.Source is JsonElement jsonElement)
            return JsonSerializer.Deserialize<T>(jsonElement.GetRawText(), _jsonSerializerOptions);

        var json = JsonSerializer.Serialize(_hit.Source);
        return JsonSerializer.Deserialize<T>(json, _jsonSerializerOptions);
    }

    public object As(Type objectType)
    {
        if (_hit?.Source == null)
            return null;

        if (objectType.IsInstanceOfType(_hit.Source))
            return _hit.Source;

        if (_hit.Source is JsonElement jsonElement)
            return JsonSerializer.Deserialize(jsonElement.GetRawText(), objectType, _jsonSerializerOptions);

        var json = JsonSerializer.Serialize(_hit.Source);
        return JsonSerializer.Deserialize(json, objectType, _jsonSerializerOptions);
    }
}
