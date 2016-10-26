using System;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Types;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Indexes {
    public sealed class IdentityIndex : Index {
        public IdentityIndex(IElasticConfiguration configuration) : base(configuration, "identity") {
            AddType(Identity = new IdentityType(this));
        }

        public IdentityType Identity { get; }
    }
}