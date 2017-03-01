using System;

namespace Foundatio.Repositories.Models {
    public class BeforeQueryEventArgs<T> : EventArgs where T : class, new() {
        public BeforeQueryEventArgs(IRepositoryQuery<T> query, ICommandOptions<T> options, IReadOnlyRepository<T> repository, Type resultType) {
            Query = query;
            Options = options;
            Repository = repository;
            ResultType = resultType;
        }

        public Type ResultType { get; private set; }
        public IRepositoryQuery<T> Query { get; private set; }
        public ICommandOptions<T> Options { get; private set; }
        public IReadOnlyRepository<T> Repository { get; private set; }
    }
}