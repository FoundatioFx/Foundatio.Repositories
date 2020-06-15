using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;
using Foundatio.Utility;

namespace Foundatio.Repositories {
    public interface IReadOnlyRepository<T> where T : class, new() {
        Task<T> GetByIdAsync(Id id, CommandOptionsDescriptor<T> options = null);
        Task<IReadOnlyCollection<T>> GetByIdsAsync(Ids ids, CommandOptionsDescriptor<T> options = null);
        Task<FindResults<T>> GetAllAsync(CommandOptionsDescriptor<T> options = null);
        Task<bool> ExistsAsync(Id id);
        Task<long> CountAsync(CommandOptionsDescriptor<T> options = null);
        Task InvalidateCacheAsync(T document, CommandOptionsDescriptor<T> options = null);
        Task InvalidateCacheAsync(IEnumerable<T> documents, CommandOptionsDescriptor<T> options = null);

        AsyncEvent<BeforeQueryEventArgs<T>> BeforeQuery { get; }
    }

    public static class ReadOnlyRepositoryExtensions {
        public static Task<T> GetByIdAsync<T>(this IReadOnlyRepository<T> repository, Id id, ICommandOptions options) where T : class, new() {
            return repository.GetByIdAsync(id, o => options.As<T>());
        }

        public static Task<IReadOnlyCollection<T>> GetByIdsAsync<T>(this IReadOnlyRepository<T> repository, Ids ids, ICommandOptions options) where T : class, new() {
            return repository.GetByIdsAsync(ids, o => options.As<T>());
        }

        public static Task<FindResults<T>> GetAllAsync<T>(this IReadOnlyRepository<T> repository, ICommandOptions options) where T : class, new() {
            return repository.GetAllAsync(o => options.As<T>());
        }

        public static Task<long> CountAsync<T>(this IReadOnlyRepository<T> repository, ICommandOptions options) where T : class, new() {
            return repository.CountAsync(o => options.As<T>());
        }

        public static Task InvalidateCacheAsync<T>(this IReadOnlyRepository<T> repository, T document, ICommandOptions options) where T : class, new() {
            return repository.InvalidateCacheAsync(document, o => options.As<T>());
        }

        public static Task InvalidateCacheAsync<T>(this IReadOnlyRepository<T> repository, IEnumerable<T> documents, ICommandOptions options) where T : class, new() {
            return repository.InvalidateCacheAsync(documents, o => options.As<T>());
        }
    }
}
