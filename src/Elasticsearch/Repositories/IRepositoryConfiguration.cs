using FluentValidation;
using Foundatio.Caching;
using Foundatio.Messaging;

namespace Foundatio.Repositories.Elasticsearch {
    public interface IRepositoryConfiguration<T> where T : class {
        ICacheClient Cache { get; }
        IMessagePublisher MessagePublisher { get; }
        IValidator<T> Validator { get; }
    }
}