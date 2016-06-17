using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Nest;

namespace Foundatio.Repositories.Elasticsearch {
    public interface IElasticRepositoryConfiguration<T>: IRepositoryConfiguration<T> where T : class {
        IElasticIndexType<T> Type { get; }
        IElasticIndex Index { get; }
        IElasticClient Client { get; }
        IQueryBuilder QueryBuilder { get; }
    }
}