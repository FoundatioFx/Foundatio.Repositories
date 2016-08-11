using System;
using Foundatio.Logging;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Tests.Configuration {
    public sealed class EmployeeIndex : Index {
        public EmployeeIndex(IElasticClient client, ILoggerFactory loggerFactory): base(client, "employees", loggerFactory) {
            Employee = new EmployeeType(this);
            AddType(Employee);
        }

        public EmployeeType Employee { get; }
    }

    public sealed class VersionedEmployeeIndex : VersionedIndex {
        public VersionedEmployeeIndex(IElasticClient client, int version, ILoggerFactory loggerFactory) : base(client, "employees", version, loggerFactory) {
            Employee = new EmployeeType(this);
            AddType(Employee);
        }

        public EmployeeType Employee { get; }
    }

    public sealed class DailyEmployeeIndex : DailyIndex {
        public DailyEmployeeIndex(IElasticClient client, int version, ILoggerFactory loggerFactory) : base(client, "daily-employees", version, loggerFactory) {
            Employee = new DailyEmployeeType(this);
            AddType(Employee);
            AddAlias($"{Name}-today", TimeSpan.FromDays(1));
            AddAlias($"{Name}-last7days", TimeSpan.FromDays(7));
            AddAlias($"{Name}-last30days", TimeSpan.FromDays(30));
        }

        public DailyEmployeeType Employee { get; }
    }
    
    public sealed class DailyEmployeeIndexWithWrongEmployeeType : DailyIndex {
        public DailyEmployeeIndexWithWrongEmployeeType(IElasticClient client, int version, ILoggerFactory loggerFactory) : base(client, "daily-employees", version, loggerFactory) {
            Employee = new EmployeeType(this);
            AddType(Employee);
        }
        
        public EmployeeType Employee { get; }
    }

    public sealed class MonthlyEmployeeIndex : MonthlyIndex {
        public MonthlyEmployeeIndex(IElasticClient client, int version, ILoggerFactory loggerFactory) : base(client, "monthly-employees", version, loggerFactory) {
            Employee = new MonthlyEmployeeType(this);
            AddType(Employee);
            AddAlias($"{Name}-today", TimeSpan.FromDays(1));
            AddAlias($"{Name}-last7days", TimeSpan.FromDays(7));
            AddAlias($"{Name}-last30days", TimeSpan.FromDays(30));
            AddAlias($"{Name}-last60days", TimeSpan.FromDays(60));
        }

        public MonthlyEmployeeType Employee { get; }
    }
}