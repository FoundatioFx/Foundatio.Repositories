using System;
using System.Threading.Tasks;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Configuration {
    public class Index : IndexBase {
        public Index(IElasticConfiguration configuration, string name) : base(configuration, name) {}

        public override Task ConfigureAsync() {
            return CreateIndexAsync(Name, ConfigureDescriptor);
        }
        
        public virtual CreateIndexDescriptor ConfigureDescriptor(CreateIndexDescriptor idx) {
            idx.AddAlias(Name);

            foreach (var t in IndexTypes)
                t.Configure(idx);

            return idx;
        }
    }

    public sealed class Index<T> : Index where T: class {
        public Index(IElasticConfiguration configuration, string name = null): base(configuration, name ?? typeof(T).Name.ToLower()) {
            Type = AddDynamicType<T>(Name);
        }

        public IIndexType<T> Type { get; }
    }
}