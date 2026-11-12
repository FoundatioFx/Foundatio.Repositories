namespace Foundatio.Repositories.Models;

public interface IVersioned
{
    /// <summary>
    /// The optimistic concurrency version (<c>"{primaryTerm}:{sequenceNumber}"</c>) the document was last read or written
    /// at, or <c>null</c> when it has none: a document that was never stored, or one stored in an index with sequence
    /// numbers disabled. The repository sets it from Elasticsearch metadata on every read and write; it is not stored in
    /// the document.
    /// </summary>
    string? Version { get; set; }
}
