using System;

namespace Foundatio.Repositories.Exceptions;

/// <summary>Base exception for repository configuration and operational errors.</summary>
public class RepositoryException : Exception
{
    public RepositoryException() : base() { }
    public RepositoryException(string message) : base(message) { }
    public RepositoryException(string message, Exception? innerException) : base(message, innerException) { }
}
