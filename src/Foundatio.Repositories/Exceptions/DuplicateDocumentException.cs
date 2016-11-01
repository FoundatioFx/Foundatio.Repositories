using System;

namespace Foundatio.Repositories.Exceptions {
    public class DuplicateDocumentException : ApplicationException {
        public DuplicateDocumentException(string message, Exception innerException) : base(message, innerException) {}
    }
}