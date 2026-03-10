using System;

namespace Foundatio.Repositories.Models;

/// <summary>
/// Event arguments for the <see cref="IReadOnlyRepository{T}.AfterQuery"/> event,
/// raised after a query is executed to allow transformation of query results including aggregations.
/// </summary>
/// <typeparam name="T">The document type.</typeparam>
public class AfterQueryEventArgs<T> : EventArgs where T : class, new()
{
    public AfterQueryEventArgs(IRepositoryQuery query, ICommandOptions options, IReadOnlyRepository<T> repository, Type resultType, CountResult result)
    {
        Query = query;
        Options = options;
        Repository = repository;
        ResultType = resultType;
        Result = result;
    }

    public Type ResultType { get; private set; }
    public IRepositoryQuery Query { get; private set; }
    public ICommandOptions Options { get; private set; }
    public IReadOnlyRepository<T> Repository { get; private set; }
    public CountResult Result { get; private set; }
}
