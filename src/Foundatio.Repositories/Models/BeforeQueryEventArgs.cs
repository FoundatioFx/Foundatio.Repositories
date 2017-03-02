using System;

namespace Foundatio.Repositories.Models {
    public class BeforeQueryEventArgs<T> : EventArgs where T : class, new() {
        public BeforeQueryEventArgs(IRepositoryQuery query, ICommandOptions options, IReadOnlyRepository<T> repository, Type resultType) {
            Query = query;
            Options = options;
            Repository = repository;
            ResultType = resultType;
        }

        public Type ResultType { get; private set; }
        public IRepositoryQuery Query { get; private set; }
        public ICommandOptions Options { get; private set; }
        public IReadOnlyRepository<T> Repository { get; private set; }
    }
}