using System;

namespace Foundatio.Repositories.Exceptions {
    public class RepositoryException : Exception {
        public RepositoryException() : base() { }
        public RepositoryException(string message) : base(message) { }
        public RepositoryException(string message, Exception innerException) : base(message, innerException) { }
    }

    public class DocumentException : RepositoryException {
        public DocumentException() : base() { }
        public DocumentException(string message) : base(message) { }
        public DocumentException(string message, Exception innerException) : base(message, innerException) { }
    }

    public class DuplicateDocumentException : DocumentException {
        public DuplicateDocumentException() : base() { }
        public DuplicateDocumentException(string message) : base(message) { }
        public DuplicateDocumentException(string message, Exception innerException) : base(message, innerException) {}
    }

    public class VersionConflictDocumentException : DocumentException {
        public VersionConflictDocumentException() : base() { }
        public VersionConflictDocumentException(string message) : base(message) { }
        public VersionConflictDocumentException(string message, Exception innerException) : base(message, innerException) { }
    }
}