using System;

namespace Foundatio.Repositories.Exceptions;

/// <summary>Thrown when a save or patch operation fails due to an optimistic concurrency version conflict (HTTP 409).</summary>
public class VersionConflictDocumentException : DocumentException
{
    public VersionConflictDocumentException() : base() { }
    public VersionConflictDocumentException(string message) : base(message) { }
    public VersionConflictDocumentException(string message, Exception innerException) : base(message, innerException) { }
}
