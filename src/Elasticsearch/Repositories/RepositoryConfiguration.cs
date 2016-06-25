using FluentValidation;
using Foundatio.Caching;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Messaging;
using Nest;

namespace Foundatio.Repositories.Elasticsearch {
    public class RepositoryConfiguration<T> : IElasticRepositoryConfiguration<T> where T : class {
        public RepositoryConfiguration(IElasticClient client, IIndexType<T> type = null, IElasticQueryBuilder queryBuilder = null, IValidator<T> validator = null, ICacheClient cache = null, IMessagePublisher messagePublisher = null) {
            Client = client;
            Type = type ?? new IndexType<T>();
            QueryBuilder = queryBuilder ?? Queries.Builders.ElasticQueryBuilder.Default;
            Cache = cache;
            Validator = validator;
            MessagePublisher = messagePublisher;
        }

        public IElasticClient Client { get; }
        public IIndexType<T> Type { get; }
        public IIndex Index => Type.Index;
        public IElasticQueryBuilder QueryBuilder { get; }
        public ICacheClient Cache { get; }
        public IValidator<T> Validator { get; }
        public IMessagePublisher MessagePublisher { get; }
    }
}
