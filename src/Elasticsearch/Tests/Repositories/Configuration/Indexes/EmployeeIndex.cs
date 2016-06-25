using System;
using Foundatio.Repositories.Elasticsearch.Configuration;

namespace Foundatio.Repositories.Elasticsearch.Tests.Configuration {
    public class EmployeeIndex : Index {
        public EmployeeIndex(): base(name: "employees", version: 1) {
            Employee = new EmployeeType(this);
            IndexTypes.Add(Employee);
        }

        public EmployeeType Employee { get; }
    }
}