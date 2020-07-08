using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Repositories.Models;
using Foundatio.Utility;

namespace Foundatio.Repositories {
    public interface IReadOnlyRepository<T> where T: class, new() {
        Task<T> GetByIdAsync(Id id, CommandOptionsDescriptor<T> options);
        Task<T> GetByIdAsync(Id id, ICommandOptions options = null);

        Task<IReadOnlyCollection<T>> GetByIdsAsync(Ids ids, CommandOptionsDescriptor<T> options);
        Task<IReadOnlyCollection<T>> GetByIdsAsync(Ids ids, ICommandOptions options = null);

        Task<FindResults<T>> GetAllAsync(CommandOptionsDescriptor<T> options);
        Task<FindResults<T>> GetAllAsync(ICommandOptions options = null);

        Task<bool> ExistsAsync(Id id);

        Task<long> CountAsync(CommandOptionsDescriptor<T> options);
        Task<long> CountAsync(ICommandOptions options = null);

        Task InvalidateCacheAsync(T document, CommandOptionsDescriptor<T> options);
        Task InvalidateCacheAsync(T document, ICommandOptions options = null);

        Task InvalidateCacheAsync(IEnumerable<T> documents, CommandOptionsDescriptor<T> options);
        Task InvalidateCacheAsync(IEnumerable<T> documents, ICommandOptions options = null);

        AsyncEvent<BeforeQueryEventArgs<T>> BeforeQuery { get; }
    }
}
