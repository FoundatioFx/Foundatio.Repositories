using System;
using Foundatio.Caching;
using Foundatio.Logging;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Types;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Indexes {
    public sealed class ParentChildIndex : Index {
        public ParentChildIndex(IElasticClient client, ICacheClient cache = null, ILoggerFactory loggerFactory = null): base(client, "parentchild", cache, loggerFactory) {
            Parent = new ParentType(this);
            Child = new ChildType(this);

            AddType(Parent);
            AddType(Child);
        }

        public ParentType Parent { get; }
        public ChildType Child { get; }
    }
}
