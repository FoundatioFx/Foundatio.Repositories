using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories;

public interface IFileAccessHistoryRepository : ISearchableRepository<FileAccessHistory> { }

public class FileAccessHistoryRepository : ElasticRepositoryBase<FileAccessHistory>, IFileAccessHistoryRepository
{
    public FileAccessHistoryRepository(DailyIndex<FileAccessHistory> dailyIndex) : base(dailyIndex)
    {
    }

    public FileAccessHistoryRepository(MonthlyIndex<FileAccessHistory> monthlyIndex) : base(monthlyIndex)
    {
    }
}
