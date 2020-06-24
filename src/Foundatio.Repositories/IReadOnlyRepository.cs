using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Repositories.Models;
using Foundatio.Utility;

namespace Foundatio.Repositories {
    public interface IReadOnlyRepository<T> where T: class, new() {
        Task<T> GetByIdAsync(Id id, ICommandOptions options);
        Task<T> GetByIdAsync(Id id, CommandOptionsDescriptor<T> options = null);

        Task<IReadOnlyCollection<T>> GetByIdsAsync(Ids ids, ICommandOptions options);
        Task<IReadOnlyCollection<T>> GetByIdsAsync(Ids ids, CommandOptionsDescriptor<T> options = null);

        Task<FindResults<T>> GetAllAsync(ICommandOptions options);
        Task<FindResults<T>> GetAllAsync(CommandOptionsDescriptor<T> options = null);

        Task<bool> ExistsAsync(Id id);

        Task<long> CountAsync(ICommandOptions options);
        Task<long> CountAsync(CommandOptionsDescriptor<T> options = null);
        
        Task InvalidateCacheAsync(T document, ICommandOptions options);
        Task InvalidateCacheAsync(T document, CommandOptionsDescriptor<T> options = null);

        Task InvalidateCacheAsync(IEnumerable<T> documents, ICommandOptions options);
        Task InvalidateCacheAsync(IEnumerable<T> documents, CommandOptionsDescriptor<T> options = null);

        AsyncEvent<BeforeQueryEventArgs<T>> BeforeQuery { get; }
    }
}
