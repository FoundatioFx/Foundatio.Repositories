using System;
using Foundatio.Serializer;

namespace Foundatio.Repositories.Models;

/// <summary>
/// Represents a document that can be deserialized on demand.
/// </summary>
/// <remarks>
/// Lazy documents defer deserialization until the data is actually needed, which can improve
/// performance when processing large result sets where not all documents need to be fully materialized.
/// </remarks>
public interface ILazyDocument
{
    /// <summary>
    /// Deserializes the document to the specified type.
    /// </summary>
    /// <typeparam name="T">The type to deserialize to.</typeparam>
    /// <returns>The deserialized document, or <c>null</c> if the document data is empty.</returns>
    T As<T>() where T : class;

    /// <summary>
    /// Deserializes the document to the specified type.
    /// </summary>
    /// <param name="objectType">The type to deserialize to.</param>
    /// <returns>The deserialized document, or <c>null</c> if the document data is empty.</returns>
    object As(Type objectType);
}

/// <summary>
/// Default implementation of <see cref="ILazyDocument"/> that deserializes from raw byte data.
/// </summary>
public class LazyDocument : ILazyDocument
{
    private readonly byte[] _data;
    private readonly ITextSerializer _serializer;

    /// <summary>
    /// Initializes a new instance of the <see cref="LazyDocument"/> class.
    /// </summary>
    /// <param name="data">The raw document data.</param>
    /// <param name="serializer">The serializer to use for deserialization. Defaults to JSON.</param>
    public LazyDocument(byte[] data, ITextSerializer serializer = null)
    {
        _data = data;
        _serializer = serializer ?? new JsonNetSerializer();
    }

    /// <inheritdoc/>
    public T As<T>() where T : class
    {
        if (_data == null || _data.Length == 0)
            return default;

        return _serializer.Deserialize<T>(_data);
    }

    /// <inheritdoc/>
    public object As(Type objectType)
    {
        if (_data == null || _data.Length == 0)
            return null;

        return _serializer.Deserialize(_data, objectType);
    }
}
