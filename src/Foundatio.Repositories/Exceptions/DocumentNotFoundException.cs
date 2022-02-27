using System;

namespace Foundatio.Repositories.Exceptions;

public class DocumentNotFoundException : DocumentException {
    public DocumentNotFoundException() { }

    public DocumentNotFoundException(string id) : base($"Document \"{id}\" could not be found") {
        Id = id;
    }

    public string Id { get; private set; }
}
