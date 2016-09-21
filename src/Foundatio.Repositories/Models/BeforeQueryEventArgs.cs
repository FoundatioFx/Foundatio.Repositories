using System;
using Foundatio.Repositories.Queries;

namespace Foundatio.Repositories.Models {
    public class BeforeQueryEventArgs<T> : EventArgs where T : class, new() {
        public BeforeQueryEventArgs(IRepositoryQuery query, IReadOnlyRepository<T> repository, Type resultType) {
            Query = query;
            Repository = repository;
            ResultType = resultType;
        }

        public Type ResultType { get; private set; }
        public IRepositoryQuery Query { get; private set; }
        public IReadOnlyRepository<T> Repository { get; private set; }
    }
}