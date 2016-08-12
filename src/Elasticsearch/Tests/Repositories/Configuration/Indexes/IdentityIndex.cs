using System;
using Foundatio.Caching;
using Foundatio.Logging;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Tests.Configuration {
    public sealed class IdentityIndex : Index {
        public IdentityIndex(IElasticClient client, ICacheClient cache = null, ILoggerFactory loggerFactory = null) : base(client, "identity", cache, loggerFactory) {
            Identity = new IdentityType(this);
            AddType(Identity);
        }

        public IdentityType Identity { get; }
    }
}