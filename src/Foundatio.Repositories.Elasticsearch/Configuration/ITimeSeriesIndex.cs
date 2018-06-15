using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;
using Foundatio.Repositories.Utility;

namespace Foundatio.Repositories.Elasticsearch.Configuration {
    public interface ITimeSeriesIndex : IIndex {
        string GetIndexById(Id id);
        string[] GetIndexesByQuery(IRepositoryQuery query);
        Task EnsureIndexAsync(DateTime utcDate);
        string GetIndex(DateTime utcDate);
        string[] GetIndexes(DateTime? utcStart, DateTime? utcEnd);
    }

    public interface ITimeSeriesIndex<T> : IIndex<T>, ITimeSeriesIndex where T : class {
        string GetDocumentIndex(T document);
        Task EnsureIndexAsync(T document);
    }
}