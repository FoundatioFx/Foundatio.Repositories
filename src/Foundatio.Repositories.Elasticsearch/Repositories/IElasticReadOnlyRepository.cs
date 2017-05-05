using System.Threading.Tasks;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories.Elasticsearch
{
    public interface IElasticReadOnlyRepository<T> : ISearchableReadOnlyRepository<T> where T : class, new() {
        Task<FindResults<T>> FindAsync(RepositoryQueryDescriptor<T> query, CommandOptionsDescriptor<T> options = null);
        Task<FindResults<T>> FindAsync(IRepositoryQuery query, ICommandOptions options = null);
        Task<FindResults<TResult>> FindAsAsync<TResult>(RepositoryQueryDescriptor<T> query, CommandOptionsDescriptor<T> options = null) where TResult : class, new();
        Task<FindResults<TResult>> FindAsAsync<TResult>(IRepositoryQuery query, ICommandOptions options = null) where TResult : class, new();
        Task<FindHit<T>> FindOneAsync(RepositoryQueryDescriptor<T> query, CommandOptionsDescriptor<T> options = null);
        Task<FindHit<T>> FindOneAsync(IRepositoryQuery query, ICommandOptions options = null);
        Task<bool> ExistsAsync(RepositoryQueryDescriptor<T> query);
        Task<bool> ExistsAsync(IRepositoryQuery query);
        Task<CountResult> CountAsync(RepositoryQueryDescriptor<T> query, CommandOptionsDescriptor<T> options = null);
        Task<CountResult> CountAsync(IRepositoryQuery query, ICommandOptions options = null);
    }
}