using System;
using Foundatio.Logging;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Tests.Configuration {
    public class EmployeeIndex : Index {
        public EmployeeIndex(IElasticClient client, ILoggerFactory loggerFactory): base(client, "employees", loggerFactory) {
            Employee = new EmployeeType(this);
            AddType(Employee);
        }

        public EmployeeType Employee { get; }
    }
}