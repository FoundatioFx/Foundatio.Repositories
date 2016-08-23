using System;
using System.Threading.Tasks;
using Foundatio.Caching;
using Foundatio.Logging;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Configuration {
    public class Index : IndexBase {
        public Index(IElasticClient client, string name, ICacheClient cache = null, ILoggerFactory loggerFactory = null) : base(client, name, cache, loggerFactory) {}

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
        public Index(IElasticClient client, string name = null, ICacheClient cache = null, ILoggerFactory loggerFactory = null): base(client, name ?? typeof(T).Name.ToLower(), cache, loggerFactory) {
            Type = new DynamicIndexType(this, name);
            AddType(Type);
        }

        public DynamicIndexType Type { get; }
    }
}