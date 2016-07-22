using System;
using Foundatio.Logging;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Tests.Configuration {
    public class IdentityIndex : Index {
        public IdentityIndex(IElasticClient client, ILoggerFactory loggerFactory) : base(client, "identity", loggerFactory) {
            Identity = new IdentityType(this);
            AddType(Identity);
        }

        public IdentityType Identity { get; }
    }
}