using Foundatio.Repositories.Models;

namespace Foundatio.Repositories.Elasticsearch.Repositories {
    public interface IElasticFindResults<out T> : IFindResults<T> where T : class {
        string ScrollId { get; set; }
    }

    public interface IElasticFindHit<out T> : IFindHit<T> {
        string Index { get; }
        string Type { get; }
    }
}