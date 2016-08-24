using System;
using Foundatio.Logging;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Indexes {
    public class ParentChildIndex : Index {
        public ParentChildIndex(IElasticClient client, ILoggerFactory loggerFactory): base(client, null, null, loggerFactory) {}
    }
}
