using System;

namespace Foundatio.Repositories.Models;

public class BeforeGetEventArgs<T> : EventArgs where T : class {
    public BeforeGetEventArgs(Ids ids, ICommandOptions options, IReadOnlyRepository<T> repository, Type resultType) {
        Ids = ids;
        Options = options;
        Repository = repository;
        ResultType = resultType;
    }

    public Type ResultType { get; private set; }
    public Ids Ids { get; private set; }
    public ICommandOptions Options { get; private set; }
    public IReadOnlyRepository<T> Repository { get; private set; }
}
