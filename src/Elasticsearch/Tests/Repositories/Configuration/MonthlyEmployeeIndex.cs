using System;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Models;

namespace Foundatio.Repositories.Elasticsearch.Tests.Configuration {
    public class EmployeeWithDateIndex : TemplatedElasticIndex {
        public EmployeeWithDateIndex() : base("monthly_employees") {
            AddIndexType<Employee>(new EmployeeType(this));
        }
    }
}