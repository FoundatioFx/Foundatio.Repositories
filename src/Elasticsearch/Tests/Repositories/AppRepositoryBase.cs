using System;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories.Elasticsearch.Tests {
    public abstract class AppRepositoryBase<T> : ElasticRepositoryBase<T> where T : class, IIdentity, new() {
        public AppRepositoryBase(ElasticRepositoryConfiguration<T> configuration) : base(configuration) { }
    }
}