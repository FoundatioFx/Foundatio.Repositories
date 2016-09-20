using System;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Types;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Indexes {
    public sealed class ParentChildIndex : VersionedIndex {
        public ParentChildIndex(IElasticConfiguration configuration): base(configuration, "parentchild", 1) {
            AddType(Parent = new ParentType(this));
            AddType(Child = new ChildType(this));
        }

        public ParentType Parent { get; }
        public ChildType Child { get; }
    }
}
