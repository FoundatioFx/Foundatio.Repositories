namespace Foundatio.Repositories.Exceptions;

/// <summary>Thrown when a document fails validation before being persisted.</summary>
public class DocumentValidationException : DocumentException
{
    public DocumentValidationException() { }

    public DocumentValidationException(string message) : base(message) { }
}
