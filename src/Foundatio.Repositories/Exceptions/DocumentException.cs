using System;

namespace Foundatio.Repositories.Exceptions;

/// <summary>Base exception thrown by repository operations when an Elasticsearch request fails.</summary>
public class DocumentException : RepositoryException
{
    public DocumentException() : base() { }
    public DocumentException(string message) : base(message) { }
    public DocumentException(string message, Exception? innerException) : base(message, innerException) { }
}
