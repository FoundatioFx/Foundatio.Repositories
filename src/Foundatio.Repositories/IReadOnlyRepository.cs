using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;
using Foundatio.Utility;

namespace Foundatio.Repositories {
    public interface IReadOnlyRepository<T> where T : class, new() {
        Task InvalidateCacheAsync(IEnumerable<T> documents, ICommandOptions options = null);
        Task<long> CountAsync(ICommandOptions options = null);
        Task<T> GetByIdAsync(string id, ICommandOptions options = null);
        Task<IReadOnlyCollection<T>> GetByIdsAsync(IEnumerable<string> ids, ICommandOptions options = null);
        Task<FindResults<T>> GetAllAsync(ICommandOptions options = null);
        Task<bool> ExistsAsync(string id);

        AsyncEvent<BeforeQueryEventArgs<T>> BeforeQuery { get; }        
    }

    public static class ReadOnlyRepositoryExtensions {
        public static Task<T> GetByIdAsync<T>(this IReadOnlyRepository<T> repository, string id, bool useCache = false, TimeSpan? expiresIn = null) where T : class, new() {
            return repository.GetByIdAsync(id, new CommandOptions().UseAutoCache(useCache).WithExpiresIn(expiresIn));
        }

        public static Task<IReadOnlyCollection<T>> GetByIdsAsync<T>(this IReadOnlyRepository<T> repository, IEnumerable<string> ids, bool useCache = false, TimeSpan? expiresIn = null) where T : class, new() {
            return repository.GetByIdsAsync(ids, new CommandOptions().UseAutoCache(useCache).WithExpiresIn(expiresIn));
        }

        public static Task<FindResults<T>> GetAllAsync<T>(this IReadOnlyRepository<T> repository, PagingOptions options) where T : class, new() {
            return repository.GetAllAsync(options);
        }
    }
}
