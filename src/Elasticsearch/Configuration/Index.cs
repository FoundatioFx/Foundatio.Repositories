using System;
using System.Threading.Tasks;
using Foundatio.Caching;
using Foundatio.Logging;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Configuration {
    public class Index : IndexBase {
        public Index(IElasticConfiguration elasticConfiguration, string name) : base(elasticConfiguration, name) {}

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
        public Index(IElasticConfiguration elasticConfiguration, string name = null, ICacheClient cache = null, ILoggerFactory loggerFactory = null): base(elasticConfiguration, name ?? typeof(T).Name.ToLower()) {
            Type = AddDynamicType<T>(Name);
        }

        public IIndexType<T> Type { get; }
    }
}