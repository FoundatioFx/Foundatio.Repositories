using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Repositories.Models;
using Foundatio.Utility;

namespace Foundatio.Repositories {
    public interface IReadOnlyRepository<T> where T : class, new() {
        Task<T> GetAsync(Id id, CommandOptionsDescriptor<T> options = null);
        Task<IReadOnlyCollection<T>> GetAsync(Ids ids, CommandOptionsDescriptor<T> options = null);
        Task<QueryResults<T>> GetAllAsync(CommandOptionsDescriptor<T> options = null);
        Task<bool> ExistsAsync(Id id);
        Task<long> CountAsync(CommandOptionsDescriptor<T> options = null);
        Task InvalidateCacheAsync(T document, CommandOptionsDescriptor<T> options = null);
        Task InvalidateCacheAsync(IEnumerable<T> documents, CommandOptionsDescriptor<T> options = null);

        AsyncEvent<BeforeQueryEventArgs<T>> BeforeQuery { get; }
    }
}
