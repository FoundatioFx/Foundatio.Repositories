namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Queries {
    public class MyAppSystemFilter : ISystemFilter {
        public SoftDeleteQueryMode SoftDeleteMode { get; set; }

        IRepositoryQuery ISystemFilter.GetQuery() {
            return new RepositoryQuery().SoftDeleteMode(SoftDeleteMode);
        }
    }
}
