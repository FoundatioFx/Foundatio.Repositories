using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories {
    public interface IReadOnlyRepository<T> where T : class, new() {
        Task InvalidateCacheAsync(ICollection<T> documents);
        Task<long> CountAsync();
        Task<T> GetByIdAsync(string id, bool useCache = false, TimeSpan? expiresIn = null);
        Task<FindResults<T>> GetByIdsAsync(ICollection<string> ids, bool useCache = false, TimeSpan? expiresIn = null);
        Task<FindResults<T>> GetAllAsync(SortingOptions sorting = null, PagingOptions paging = null);
        Task<bool> ExistsAsync(string id);
    }
}
