using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Repositories.Models;
using Foundatio.Utility;

namespace Foundatio.Repositories;

public interface IReadOnlyRepository<T> where T: class, new() {
    Task<T> GetByIdAsync(Id id, CommandOptionsDescriptor<T> options);
    Task<T> GetByIdAsync(Id id, ICommandOptions options = null);

    Task<IReadOnlyCollection<T>> GetByIdsAsync(Ids ids, CommandOptionsDescriptor<T> options);
    Task<IReadOnlyCollection<T>> GetByIdsAsync(Ids ids, ICommandOptions options = null);

    Task<FindResults<T>> GetAllAsync(CommandOptionsDescriptor<T> options);
    Task<FindResults<T>> GetAllAsync(ICommandOptions options = null);

    Task<bool> ExistsAsync(Id id, CommandOptionsDescriptor<T> options);
    Task<bool> ExistsAsync(Id id, ICommandOptions options = null);

    Task<CountResult> CountAsync(CommandOptionsDescriptor<T> options);
    Task<CountResult> CountAsync(ICommandOptions options = null);

    Task InvalidateCacheAsync(T document);
    Task InvalidateCacheAsync(IEnumerable<T> documents);

    Task InvalidateCacheAsync(string cacheKey);
    Task InvalidateCacheAsync(IEnumerable<string> cacheKeys);

    AsyncEvent<BeforeQueryEventArgs<T>> BeforeQuery { get; }
}
