using System;
using System.Threading.Tasks;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories.Elasticsearch
{
    public interface IElasticRepository<T> : IRepository<T> where T : class, IIdentity, new() {
        Task<long> PatchAllAsync(RepositoryQueryDescriptor<T> query, IPatchOperation operation, CommandOptionsDescriptor<T> options = null);
        Task<long> PatchAllAsync(IRepositoryQuery query, IPatchOperation operation, ICommandOptions options = null);
        Task<long> RemoveAllAsync(RepositoryQueryDescriptor<T> query, CommandOptionsDescriptor<T> options = null);
        Task<long> RemoveAllAsync(IRepositoryQuery query, ICommandOptions options = null);
        Task<long> BatchProcessAsync(RepositoryQueryDescriptor<T> query, Func<FindResults<T>, Task<bool>> processAsync, CommandOptionsDescriptor<T> options = null);
        Task<long> BatchProcessAsync(IRepositoryQuery query, Func<FindResults<T>, Task<bool>> processAsync, ICommandOptions options = null);
        Task<long> BatchProcessAsAsync<TResult>(IRepositoryQuery query, Func<FindResults<TResult>, Task<bool>> processAsync, ICommandOptions options = null)
            where TResult : class, new();
    }
}