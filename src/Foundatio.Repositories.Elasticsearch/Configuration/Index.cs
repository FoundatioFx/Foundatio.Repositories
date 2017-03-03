using System.Threading.Tasks;
using Foundatio.Repositories.Extensions;

namespace Foundatio.Repositories.Elasticsearch.Configuration {
    public class Index : IndexBase {
        public Index(IElasticConfiguration configuration, string name) : base(configuration, name) {}

        public override async Task ConfigureAsync() {
            await base.ConfigureAsync().AnyContext();
            await CreateIndexAsync(Name, ConfigureIndex).AnyContext();
        }
    }

    public sealed class Index<T> : Index where T: class {
        public Index(IElasticConfiguration configuration, string name = null): base(configuration, name ?? typeof(T).Name.ToLower()) {
            Type = AddDynamicType<T>(Name);
        }

        public IIndexType<T> Type { get; }
    }
}