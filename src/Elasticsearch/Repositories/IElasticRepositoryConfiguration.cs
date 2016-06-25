using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Nest;

namespace Foundatio.Repositories.Elasticsearch {
    public interface IElasticRepositoryConfiguration<T>: IRepositoryConfiguration<T> where T : class {
        IIndexType<T> Type { get; }
        IIndex Index { get; }
        IElasticClient Client { get; }
        IElasticQueryBuilder QueryBuilder { get; }
    }
}