using FluentValidation;
using Foundatio.Caching;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Messaging;
using Nest;

namespace Foundatio.Repositories.Elasticsearch {
    public class ElasticRepositoryConfiguration<T> : IElasticRepositoryConfiguration<T> where T : class {
        public ElasticRepositoryConfiguration(IElasticClient client, IElasticIndexType<T> elasticType = null, IQueryBuilder queryBuilder = null, IValidator<T> validator = null, ICacheClient cache = null, IMessagePublisher messagePublisher = null) {
            Client = client;
            Type = elasticType ?? new ElasticIndexType<T>();
            QueryBuilder = queryBuilder ?? new ElasticQueryBuilder();
            Cache = cache;
            Validator = validator;
            MessagePublisher = messagePublisher;
        }

        public IElasticIndexType<T> Type { get; }
        public IElasticIndex Index => Type.Index;
        public IElasticClient Client { get; }
        public IQueryBuilder QueryBuilder { get; }
        public ICacheClient Cache { get; }
        public IValidator<T> Validator { get; }
        public IMessagePublisher MessagePublisher { get; }
    }
}
