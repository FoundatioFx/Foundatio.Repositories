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
        public static Task InvalidateCacheAsync<T>(this IReadOnlyRepository<T> repository, IEnumerable<T> documents, CommandOptionsDescriptor<T> options = null) where T : class, new() {
            return repository.InvalidateCacheAsync(documents, options.Configure());
        }

        public static Task<long> CountAsync<T>(this IReadOnlyRepository<T> repository, CommandOptionsDescriptor<T> options = null) where T : class, new() {
            return repository.CountAsync(options.Configure());
        }

        public static Task<T> GetByIdAsync<T>(this IReadOnlyRepository<T> repository, Id id, CommandOptionsDescriptor<T> options = null) where T : class, new() {
            return repository.GetByIdAsync(id, options.Configure());
        }

        public static Task<IReadOnlyCollection<T>> GetByIdsAsync<T>(this IReadOnlyRepository<T> repository, Ids ids, CommandOptionsDescriptor<T> options = null) where T : class, new() {
            return repository.GetByIdsAsync(ids, options.Configure());
        }

        public static Task<FindResults<T>> GetAllAsync<T>(this IReadOnlyRepository<T> repository, CommandOptionsDescriptor<T> options = null) where T : class, new() {
            return repository.GetAllAsync(options.Configure());
        }
    }
}
