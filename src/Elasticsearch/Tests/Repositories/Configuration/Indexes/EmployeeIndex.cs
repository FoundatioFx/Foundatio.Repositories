using System;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Tests.Configuration {
    public class EmployeeIndex : Index {
        public EmployeeIndex(IElasticClient client): base(name: "employees", client: client) {
            Employee = new EmployeeType(this);
            AddType(Employee);
        }

        public EmployeeType Employee { get; }
    }
}