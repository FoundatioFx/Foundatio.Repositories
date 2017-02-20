using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Repositories.Models;
using Foundatio.Utility;

namespace Foundatio.Repositories {
    public interface IReadOnlyRepository<T> where T : class, new() {
        Task InvalidateCacheAsync(IEnumerable<T> documents);
        Task<long> CountAsync();
        Task<T> GetByIdAsync(Id id, bool useCache = false, TimeSpan? expiresIn = null);
        Task<IReadOnlyCollection<T>> GetByIdsAsync(Ids ids, bool useCache = false, TimeSpan? expiresIn = null);
        Task<FindResults<T>> GetAllAsync(PagingOptions paging = null);
        Task<bool> ExistsAsync(Id id);

        AsyncEvent<BeforeQueryEventArgs<T>> BeforeQuery { get; }        
    }

    public static class ReadOnlyRepositoryExtensions {
        public static Task<IReadOnlyCollection<T>> GetByIdsAsync<T>(this IRepository<T> repository, IEnumerable<string> ids, bool useCache = false, TimeSpan? expiresIn = null) where T : class, IIdentity, new() {
            return repository.GetByIdsAsync(new Ids(ids), useCache, expiresIn);
        }
    }
}
