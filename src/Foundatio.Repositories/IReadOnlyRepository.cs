using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Foundatio.Repositories.Models;
using Foundatio.Utility;

namespace Foundatio.Repositories {
    public delegate IRepositoryQuery<T> RepositoryQueryDescriptor<T>(IRepositoryQuery<T> query) where T : class;
    public delegate ICommandOptions<T> CommandOptionsDescriptor<T>(ICommandOptions<T> options) where T : class;

    public interface IReadOnlyRepository<T> where T : class, new() {
        Task InvalidateCacheAsync(IEnumerable<T> documents, ICommandOptions<T> options = null);
        Task<long> CountAsync(ICommandOptions<T> options = null);
        Task<T> GetByIdAsync(Id id, ICommandOptions<T> options = null);
        Task<IReadOnlyCollection<T>> GetByIdsAsync(Ids ids, ICommandOptions<T> options = null);
        Task<FindResults<T>> GetAllAsync(ICommandOptions<T> options = null);
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

        public static Task<IReadOnlyCollection<T>> GetByIdsAsync<T>(this IReadOnlyRepository<T> repository, IEnumerable<string> ids, ICommandOptions<T> options = null) where T : class, new() {
            return repository.GetByIdsAsync(new Ids(ids), options);
        }

        public static Task<IReadOnlyCollection<T>> GetByIdsAsync<T>(this IReadOnlyRepository<T> repository, IEnumerable<string> ids, CommandOptionsDescriptor<T> options = null) where T : class, new() {
            return repository.GetByIdsAsync(new Ids(ids), options.Configure());
        }

        public static Task<FindResults<T>> GetAllAsync<T>(this IReadOnlyRepository<T> repository, CommandOptionsDescriptor<T> options = null) where T : class, new() {
            return repository.GetAllAsync(options.Configure());
        }

        public static ICommandOptions<T> Configure<T>(this CommandOptionsDescriptor<T> configure) where T: class {
            ICommandOptions<T> o = new CommandOptions<T>();
            if (configure != null)
                o = configure(o);

            return o;
        }

        public static IRepositoryQuery<T> Configure<T>(this RepositoryQueryDescriptor<T> configure) where T : class {
            IRepositoryQuery<T> o = new RepositoryQuery<T>();
            if (configure != null)
                o = configure(o);

            return o;
        }
    }
}
