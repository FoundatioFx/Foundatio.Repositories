using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Repositories.Models;
using Foundatio.Utility;

namespace Foundatio.Repositories {
    public interface IReadOnlyRepository<T> where T : class, new() {
        Task InvalidateCacheAsync(IEnumerable<T> documents, ICommandOptions options = null);
        Task<long> CountAsync(ICommandOptions options = null);
        Task<T> GetByIdAsync(Id id, ICommandOptions options = null);
        Task<IReadOnlyCollection<T>> GetByIdsAsync(Ids ids, ICommandOptions options = null);
        Task<FindResults<T>> GetAllAsync(ICommandOptions options = null);
        Task<bool> ExistsAsync(Id id);

        AsyncEvent<BeforeQueryEventArgs<T>> BeforeQuery { get; }        
    }

    public static class ReadOnlyRepositoryExtensions {
        public static Task<IReadOnlyCollection<T>> GetByIdsAsync<T>(this IReadOnlyRepository<T> repository, IEnumerable<string> ids, ICommandOptions options = null) where T : class, new() {
            return repository.GetByIdsAsync(new Ids(ids), options);
        }
    }
}
