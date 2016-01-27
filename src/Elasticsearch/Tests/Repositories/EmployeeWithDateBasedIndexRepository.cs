using System;
using Exceptionless.DateTimeExtensions;
using Foundatio.Elasticsearch.Repositories;
using Foundatio.Repositories.Elasticsearch.Tests.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Models;
using Foundatio.Repositories.Utility;

namespace Foundatio.Repositories.Elasticsearch.Tests {
    public class EmployeeWithDateBasedIndexRepository : AppRepositoryBase<EmployeeWithDate> {
        private readonly EmployeeWithDateIndex _index;
        public EmployeeWithDateBasedIndexRepository(ElasticRepositoryContext<EmployeeWithDate> context, EmployeeWithDateIndex index) : base(context) {
            _index = index;

            GetDocumentIdFunc = GetDocumentId;
            GetDocumentIndexFunc = employee => GetIndexById(employee.Id);
        }

        private string GetDocumentId(EmployeeWithDate employee) {
            // if date falls in the current months index then return a new object id.
            var date = employee.StartDate.ToUniversalTime();
            if (date.IntersectsMonth(DateTime.UtcNow))
                return ObjectId.GenerateNewId().ToString();

            // GenerateNewId will translate it to utc.
            return ObjectId.GenerateNewId(employee.StartDate.DateTime).ToString();
        }

        protected override string GetIndexById(string id) {
            ObjectId objectId;
            if (ObjectId.TryParse(id, out objectId))
                return String.Concat(_index.VersionedName, "-", objectId.CreationTime.ToString("yyyyMM"));

            return null;
        }
    }
}