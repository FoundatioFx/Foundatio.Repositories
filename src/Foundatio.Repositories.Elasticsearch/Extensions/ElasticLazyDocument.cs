using System;
using System.IO;
using System.Reflection;
using Elasticsearch.Net;
using Nest;
using ILazyDocument = Foundatio.Repositories.Models.ILazyDocument;

namespace Foundatio.Repositories.Elasticsearch.Extensions;

public class ElasticLazyDocument : ILazyDocument
{
    private readonly Nest.ILazyDocument _inner;
    private IElasticsearchSerializer _requestResponseSerializer;

    public ElasticLazyDocument(Nest.ILazyDocument inner)
    {
        _inner = inner;
    }

    private static readonly Lazy<Func<Nest.ILazyDocument, IElasticsearchSerializer>> _getSerializer =
        new(() =>
        {
            var serializerField = typeof(Nest.LazyDocument).GetField("_requestResponseSerializer", BindingFlags.GetField | BindingFlags.NonPublic | BindingFlags.Instance);
            return lazyDocument =>
            {
                var d = lazyDocument as Nest.LazyDocument;
                if (d == null)
                    return null;

                var serializer = serializerField?.GetValue(d) as IElasticsearchSerializer;
                return serializer;
            };
        });

    private static readonly Lazy<Func<Nest.ILazyDocument, byte[]>> _getBytes =
        new(() =>
        {
            var bytesProperty = typeof(Nest.LazyDocument).GetProperty("Bytes", BindingFlags.GetProperty | BindingFlags.NonPublic | BindingFlags.Instance);
            return lazyDocument =>
            {
                var d = lazyDocument as Nest.LazyDocument;
                if (d == null)
                    return null;

                var bytes = bytesProperty?.GetValue(d) as byte[];
                return bytes;
            };
        });

    public T As<T>() where T : class
    {
        if (_requestResponseSerializer == null)
            _requestResponseSerializer = _getSerializer.Value(_inner);

        var bytes = _getBytes.Value(_inner);
        var hit = _requestResponseSerializer.Deserialize<IHit<T>>(new MemoryStream(bytes));
        return hit?.Source;
    }

    public object As(Type objectType)
    {
        var hitType = typeof(IHit<>).MakeGenericType(objectType);
        return _inner.As(hitType);
    }
}
