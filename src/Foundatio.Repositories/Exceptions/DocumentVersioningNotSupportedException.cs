using System;

namespace Foundatio.Repositories.Exceptions;

/// <summary>
/// Thrown when a write needs optimistic concurrency control (or the Update API) but the target index does not support it,
/// for example an Elasticsearch index created with <c>index.disable_sequence_numbers: true</c> (the default for time series
/// data stream backing indexes in Elasticsearch 9.4 and later).
/// </summary>
/// <remarks>
/// Versioned (<c>IVersioned</c>) documents need sequence numbers. Store them in an index with sequence numbers enabled, or
/// skip the version check for writes that are allowed to overwrite.
/// </remarks>
public class DocumentVersioningNotSupportedException : DocumentException
{
    public DocumentVersioningNotSupportedException() : base() { }
    public DocumentVersioningNotSupportedException(string message) : base(message) { }
    public DocumentVersioningNotSupportedException(string message, Exception? innerException) : base(message, innerException) { }
}
