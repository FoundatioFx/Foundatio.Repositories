using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories; 

public interface IFileAccessHistoryRepository : ISearchableRepository<FileAccessHistory> {}

public class FileAccessHistoryRepository : ElasticRepositoryBase<FileAccessHistory>, IFileAccessHistoryRepository {
    public FileAccessHistoryRepository(MyAppElasticConfiguration elasticConfiguration) : base(elasticConfiguration.MonthlyFileAccessHistory) {
    }
}