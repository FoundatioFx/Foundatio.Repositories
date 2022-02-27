using System;

namespace Foundatio.Repositories.Exceptions;

public class DocumentException : RepositoryException {
    public DocumentException() : base() { }
    public DocumentException(string message) : base(message) { }
    public DocumentException(string message, Exception innerException) : base(message, innerException) { }
}
