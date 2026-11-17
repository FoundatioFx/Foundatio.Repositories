namespace Foundatio.Repositories.Models;

public interface IVersioned
{
    /// <summary>
    /// The optimistic concurrency version the document was last read or written at, or <c>null</c> when none is available.
    /// </summary>
    /// <remarks>
    /// The Elasticsearch provider uses <c>"{primaryTerm}:{sequenceNumber}"</c> and sets this property from response
    /// metadata or the cached hit's version. The model controls whether the property appears in serialized JSON;
    /// implement it explicitly or apply the serializer's ignore attribute to omit it.
    /// </remarks>
    string? Version { get; set; }
}
