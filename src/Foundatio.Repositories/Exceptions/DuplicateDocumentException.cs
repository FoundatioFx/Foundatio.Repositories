using System;

namespace Foundatio.Repositories.Exceptions;

/// <summary>Thrown when an add operation encounters a document with a duplicate ID (HTTP 409 on create).</summary>
public class DuplicateDocumentException : DocumentException
{
    public DuplicateDocumentException() : base() { }
    public DuplicateDocumentException(string message) : base(message) { }
    public DuplicateDocumentException(string message, Exception? innerException) : base(message, innerException) { }
}
