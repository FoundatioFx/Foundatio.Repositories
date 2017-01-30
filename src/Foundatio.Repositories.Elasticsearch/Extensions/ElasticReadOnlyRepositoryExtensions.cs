using System.Threading.Tasks;
using Foundatio.Repositories.Elasticsearch.Models;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories {
    public static class ElasticReadOnlyRepositoryExtensions {
        public static Task<FindResults<T>> GetAllAsync<T>(this IReadOnlyRepository<T> repository, ElasticPagingOptions options) where T : class, new() {
            return repository.GetAllAsync(options);
        }
    }
}
