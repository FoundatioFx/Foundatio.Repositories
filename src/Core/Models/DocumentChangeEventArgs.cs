using System;
using System.Collections.Generic;

namespace Foundatio.Repositories.Models {
    public class DocumentsChangeEventArgs<T> : EventArgs where T : class, IIdentity, new() {
        public DocumentsChangeEventArgs(ChangeType changeType, IReadOnlyCollection<ModifiedDocument<T>> documents, IRepository<T> repository) {
            ChangeType = changeType;
            Documents = documents ?? new List<ModifiedDocument<T>>();
            Repository = repository;
        }

        public ChangeType ChangeType { get; private set; }
        public IReadOnlyCollection<ModifiedDocument<T>> Documents { get; private set; }
        public IRepository<T> Repository { get; private set; }
    }

    public class DocumentsEventArgs<T> : EventArgs where T : class, IIdentity, new() {
        public DocumentsEventArgs(IReadOnlyCollection<T> documents, IRepository<T> repository) {
            Documents = documents ?? new List<T>();
            Repository = repository;
        }

        public IReadOnlyCollection<T> Documents { get; private set; }
        public IRepository<T> Repository { get; private set; }
    }

    public class ModifiedDocumentsEventArgs<T> : EventArgs where T : class, IIdentity, new() {
        public ModifiedDocumentsEventArgs(IReadOnlyCollection<ModifiedDocument<T>> documents, IRepository<T> repository) {
            Documents = documents ?? new List<ModifiedDocument<T>>();
            Repository = repository;
        }

        public IReadOnlyCollection<ModifiedDocument<T>> Documents { get; private set; }
        public IRepository<T> Repository { get; private set; }
    }

    public class ModifiedDocument<T> where T : class, new() {
        public ModifiedDocument(T value, T original) {
            Value = value;
            Original = original;
        }

        public T Value { get; private set; }
        public T Original { get; private set; }
    }
}
