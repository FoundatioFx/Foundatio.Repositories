using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Types;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Indexes {
    public sealed class ParentChildIndex : VersionedIndex {
        public ParentChildIndex(IElasticConfiguration configuration): base(configuration, "parentchild", 1) {
            AddType(Parent = new ParentType(this));
            AddType(Child = new ChildType(this));
        }

        public ParentType Parent { get; }
        public ChildType Child { get; }

        public override CreateIndexDescriptor ConfigureIndex(CreateIndexDescriptor idx) {
            return base.ConfigureIndex(idx.Settings(s => s.NumberOfReplicas(0).NumberOfShards(1)));
        }
    }
}
