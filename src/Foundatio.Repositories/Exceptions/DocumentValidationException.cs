using System;

namespace Foundatio.Repositories.Exceptions;

public class DocumentValidationException : DocumentException {
    public DocumentValidationException() { }

    public DocumentValidationException(string message) : base(message) {}
}
