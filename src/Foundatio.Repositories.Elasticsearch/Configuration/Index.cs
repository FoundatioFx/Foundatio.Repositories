using System;
using System.Threading.Tasks;

namespace Foundatio.Repositories.Elasticsearch.Configuration {
    public class Index : IndexBase {
        public Index(IElasticConfiguration configuration, string name) : base(configuration, name) {}

        public override Task ConfigureAsync() {
            return CreateIndexAsync(Name, ConfigureDescriptor);
        }
    }

    public sealed class Index<T> : Index where T: class {
        public Index(IElasticConfiguration configuration, string name = null): base(configuration, name ?? typeof(T).Name.ToLower()) {
            Type = AddDynamicType<T>(Name);
        }

        public IIndexType<T> Type { get; }
    }
}