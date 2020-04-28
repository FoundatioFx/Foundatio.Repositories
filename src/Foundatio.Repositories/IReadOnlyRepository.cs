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

        public Task InvalidateCacheAsync(T document, ICommandOptions options = null) {
            return InvalidateCacheAsync(new [] { document }, options);
        }
        
        public Task InvalidateCacheAsync(T document, CommandOptionsDescriptor<T> options) {
            return InvalidateCacheAsync(new [] { document }, options.Configure());
        }

        public Task InvalidateCacheAsync(IEnumerable<T> documents, CommandOptionsDescriptor<T> options) {
            return InvalidateCacheAsync(documents, options.Configure());
        }

        public Task<long> CountAsync(CommandOptionsDescriptor<T> options) {
            return CountAsync(options.Configure());
        }

        public Task<T> GetByIdAsync(Id id, CommandOptionsDescriptor<T> options) {
            return GetByIdAsync(id, options.Configure());
        }

        public Task<IReadOnlyCollection<T>> GetByIdsAsync(Ids ids, CommandOptionsDescriptor<T> options) {
            return GetByIdsAsync(ids, options.Configure());
        }

        public Task<FindResults<T>> GetAllAsync(CommandOptionsDescriptor<T> options) {
            return GetAllAsync(options.Configure());
        }
    }
}
