using System;
using Foundatio.Repositories.Elasticsearch.Configuration;

namespace Foundatio.Repositories.Elasticsearch.Tests.Configuration {
    public sealed class EmployeeIndex : Index {
        public EmployeeIndex(IElasticConfiguration configuration): base(configuration, "employees") {
            AddType(Employee = new EmployeeType(this));
        }

        public EmployeeType Employee { get; }
    }

    public sealed class EmployeeIndexWithYearsEmployed : Index {
        public EmployeeIndexWithYearsEmployed(IElasticConfiguration configuration) : base(configuration, "employees") {
            AddType(Employee = new EmployeeTypeWithYearsEmployed(this));
        }

        public EmployeeTypeWithYearsEmployed Employee { get; }
    }

    public sealed class VersionedEmployeeIndex : VersionedIndex {
        public VersionedEmployeeIndex(IElasticConfiguration configuration, int version) : base(configuration, "employees", version) {
            AddType(Employee = new EmployeeType(this));
        }

        public EmployeeType Employee { get; }
    }

    public sealed class DailyEmployeeIndex : DailyIndex {
        public DailyEmployeeIndex(IElasticConfiguration configuration, int version) : base(configuration, "daily-employees", version) {
            AddType(Employee = new DailyEmployeeType(this));
            AddAlias($"{Name}-today", TimeSpan.FromDays(1));
            AddAlias($"{Name}-last7days", TimeSpan.FromDays(7));
            AddAlias($"{Name}-last30days", TimeSpan.FromDays(30));
        }

        public DailyEmployeeType Employee { get; }
    }
    
    public sealed class DailyEmployeeIndexWithWrongEmployeeType : DailyIndex {
        public DailyEmployeeIndexWithWrongEmployeeType(IElasticConfiguration configuration, int version) : base(configuration, "daily-employees", version) {
            AddType(Employee = new EmployeeType(this));
        }
        
        public EmployeeType Employee { get; }
    }

    public sealed class MonthlyEmployeeIndex : MonthlyIndex {
        public MonthlyEmployeeIndex(IElasticConfiguration configuration, int version) : base(configuration, "monthly-employees", version) {
            AddType(Employee = new MonthlyEmployeeType(this));
            AddAlias($"{Name}-today", TimeSpan.FromDays(1));
            AddAlias($"{Name}-last7days", TimeSpan.FromDays(7));
            AddAlias($"{Name}-last30days", TimeSpan.FromDays(30));
            AddAlias($"{Name}-last60days", TimeSpan.FromDays(60));
        }

        public MonthlyEmployeeType Employee { get; }
    }
}