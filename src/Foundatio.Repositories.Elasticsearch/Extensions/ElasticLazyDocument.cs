using System;
using System.Text.Json;
using Elastic.Clients.Elasticsearch.Core.Search;
using Foundatio.Serializer;
using ILazyDocument = Foundatio.Repositories.Models.ILazyDocument;

namespace Foundatio.Repositories.Elasticsearch.Extensions;

public class ElasticLazyDocument : ILazyDocument
{
    private readonly Hit<object> _hit;
    private readonly ITextSerializer _serializer;

    public ElasticLazyDocument(Hit<object> hit, ITextSerializer serializer)
    {
        _hit = hit;
        _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
    }

    public T? As<T>() where T : class
    {
        if (_hit?.Source is null)
            return null;

        if (_hit.Source is T typed)
            return typed;

        if (_hit.Source is JsonElement jsonElement)
            return _serializer.Deserialize<T>(jsonElement.GetRawText());

        return _serializer.Deserialize<T>(_serializer.SerializeToString(_hit.Source));
    }

    public object? As(Type objectType)
    {
        if (_hit?.Source is null)
            return null;

        if (objectType.IsInstanceOfType(_hit.Source))
            return _hit.Source;

        if (_hit.Source is JsonElement jsonElement)
            return _serializer.Deserialize(jsonElement.GetRawText(), objectType);

        return _serializer.Deserialize(_serializer.SerializeToString(_hit.Source), objectType);
    }
}
