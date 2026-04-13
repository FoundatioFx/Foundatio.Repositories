namespace Foundatio.Repositories.Exceptions;

/// <summary>Thrown when a requested document does not exist in the repository.</summary>
public class DocumentNotFoundException : DocumentException
{
    public DocumentNotFoundException() { }

    public DocumentNotFoundException(string id) : base($"Document \"{id}\" could not be found")
    {
        Id = id;
    }

    public string? Id { get; private set; }
}
