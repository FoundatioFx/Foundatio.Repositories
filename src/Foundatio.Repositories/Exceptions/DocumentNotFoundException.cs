using System;

namespace Foundatio.Repositories.Exceptions {
    public class DocumentNotFoundException : ApplicationException {
        public DocumentNotFoundException() { }

        public DocumentNotFoundException(string id) {
            Id = id;
        }

        public string Id { get; private set; }

        public override string ToString() {
            if (!String.IsNullOrEmpty(Id))
                return $"Document \"{Id}\" could not be found";

            return base.ToString();
        }
    }
}
