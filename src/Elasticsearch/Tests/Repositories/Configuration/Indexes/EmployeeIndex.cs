using System;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Tests.Configuration {
    public class EmployeeIndex : Index {
        public EmployeeIndex(IElasticClient client): base(name: "employees", client: client, version: 1) {
            Employee = new EmployeeType(this);
            IndexTypes.Add(Employee);
        }

        public EmployeeType Employee { get; }
    }
}