namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Queries {
    public class MyAppSystemFilter : RepositoryQuery, ISystemFilter {
        IRepositoryQuery ISystemFilter.GetQuery() {
            return this;
        }
    }
}
