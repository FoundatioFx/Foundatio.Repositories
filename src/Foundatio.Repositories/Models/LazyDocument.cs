using System;
using Foundatio.Serializer;

namespace Foundatio.Repositories.Models;

public interface ILazyDocument
{
    T As<T>() where T : class;
    object As(Type objectType);
}

public class LazyDocument : ILazyDocument
{
    private readonly byte[] _data;
    private readonly ITextSerializer _serializer;

    public LazyDocument(byte[] data, ITextSerializer serializer = null)
    {
        _data = data;
        _serializer = serializer ?? new JsonNetSerializer();
    }

    public T As<T>() where T : class
    {
        if (_data == null || _data.Length == 0)
            return default;

        return _serializer.Deserialize<T>(_data);
    }

    public object As(Type objectType)
    {
        if (_data == null || _data.Length == 0)
            return null;

        return _serializer.Deserialize(_data, objectType);
    }
}
