using System;

namespace Foundatio.Repositories.Exceptions;

public class VersionConflictDocumentException : DocumentException
{
    public VersionConflictDocumentException() : base() { }
    public VersionConflictDocumentException(string message) : base(message) { }
    public VersionConflictDocumentException(string message, Exception innerException) : base(message, innerException) { }
}
