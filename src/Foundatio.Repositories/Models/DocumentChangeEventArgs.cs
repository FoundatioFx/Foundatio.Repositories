using System;
using System.Collections.Generic;

namespace Foundatio.Repositories.Models {
    public class DocumentsChangeEventArgs<T> : EventArgs where T : class, IIdentity, new() {
        public DocumentsChangeEventArgs(ChangeType changeType, IReadOnlyCollection<ModifiedDocument<T>> documents, IRepository<T> repository, ICommandOptions options) {
            ChangeType = changeType;
            Documents = documents ?? EmptyReadOnly<ModifiedDocument<T>>.Collection;
            Repository = repository;
            Options = options;
        }

        public ChangeType ChangeType { get; private set; }
        public IReadOnlyCollection<ModifiedDocument<T>> Documents { get; private set; }
        public IRepository<T> Repository { get; private set; }
        public ICommandOptions Options { get; private set; }
    }

    public class DocumentsEventArgs<T> : EventArgs where T : class, IIdentity, new() {
        public DocumentsEventArgs(IReadOnlyCollection<T> documents, IRepository<T> repository, ICommandOptions options) {
            Documents = documents ?? EmptyReadOnly<T>.Collection;
            Repository = repository;
            Options = options;
        }

        public IReadOnlyCollection<T> Documents { get; private set; }
        public IRepository<T> Repository { get; private set; }
        public ICommandOptions Options { get; private set; }
    }

    public class ModifiedDocumentsEventArgs<T> : EventArgs where T : class, IIdentity, new() {
        public ModifiedDocumentsEventArgs(IReadOnlyCollection<ModifiedDocument<T>> documents, IRepository<T> repository, ICommandOptions options) {
            Documents = documents ?? EmptyReadOnly<ModifiedDocument<T>>.Collection;
            Repository = repository;
            Options = options;
        }

        public IReadOnlyCollection<ModifiedDocument<T>> Documents { get; private set; }
        public IRepository<T> Repository { get; private set; }
        public ICommandOptions Options { get; private set; }
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
