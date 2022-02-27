using System;

namespace Foundatio.Repositories.Exceptions;

public class DuplicateDocumentException : DocumentException {
    public DuplicateDocumentException() : base() { }
    public DuplicateDocumentException(string message) : base(message) { }
    public DuplicateDocumentException(string message, Exception innerException) : base(message, innerException) {}
}
