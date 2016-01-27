using System;
using Foundatio.Elasticsearch.Repositories;
using Foundatio.Repositories.Models;

namespace Foundatio.Repositories.Elasticsearch.Tests {
    public abstract class AppRepositoryBase<T> : ElasticRepositoryBase<T> where T : class, IIdentity, new() {
        public AppRepositoryBase(ElasticRepositoryContext<T> context) : base(context) { }
    }
}