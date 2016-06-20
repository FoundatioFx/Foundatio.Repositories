using System;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Models;

namespace Foundatio.Repositories.Elasticsearch.Tests.Configuration {
    public class EmployeeIndex : Index {
        public EmployeeIndex(): base(name: "employees", version: 1) {
            Employee = new EmployeeType(this);
            AddIndexType<Employee>(Employee);
        }

        public EmployeeType Employee { get; }
    }
}