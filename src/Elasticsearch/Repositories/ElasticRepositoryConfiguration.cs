using FluentValidation;
using Foundatio.Caching;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Messaging;
using Nest;

namespace Foundatio.Repositories.Elasticsearch {
    public class ElasticRepositoryConfiguration<T> : IElasticRepositoryConfiguration<T> where T : class {
        public ElasticRepositoryConfiguration(IElasticClient client, IIndexType<T> type = null, IQueryBuilder queryBuilder = null, IValidator<T> validator = null, ICacheClient cache = null, IMessagePublisher messagePublisher = null) {
            Client = client;
            Type = type ?? new IndexType<T>();
            QueryBuilder = queryBuilder ?? new ElasticQueryBuilder();
            Cache = cache;
            Validator = validator;
            MessagePublisher = messagePublisher;
        }

        public IElasticClient Client { get; }
        public IIndexType<T> Type { get; }
        public IIndex Index => Type.Index;
        public IQueryBuilder QueryBuilder { get; }
        public ICacheClient Cache { get; }
        public IValidator<T> Validator { get; }
        public IMessagePublisher MessagePublisher { get; }
    }
}
