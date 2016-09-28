using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Elasticsearch.Tests.Models;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Tests.Configuration {
    public class EmployeeType : IndexTypeBase<Employee> {
        public EmployeeType(IIndex index) : base(index) { }

        public override TypeMappingDescriptor<Employee> BuildMapping(TypeMappingDescriptor<Employee> map) {
            return base.BuildMapping(map
                .Dynamic(false)
                .Properties(p => p
                    .SetupDefaults()
                    .Keyword(f => f.Name(e => e.Id).IndexName(Fields.Id))
                    .Keyword(f => f.Name(e => e.CompanyId).IndexName(Fields.CompanyId))
                    .Keyword(f => f.Name(e => e.CompanyName).IndexName(Fields.CompanyName))
                    .Text(f => f.Name(e => e.Name).IndexName(Fields.Name))
                    .Number(f => f.Name(e => e.Age).IndexName(Fields.Age))
                ));
        }

        protected override void ConfigureQueryBuilder(ElasticQueryBuilder builder) {
            builder.UseQueryParser(this);
        }

        public class Fields {
            public const string Id = "id";
            public const string CompanyId = "company";
            public const string CompanyName = "company_name";
            public const string Name = "name";
            public const string Age = "age";
        }
    }
    
    public class EmployeeTypeWithYearsEmployed : IndexTypeBase<Employee> {
        public EmployeeTypeWithYearsEmployed(IIndex index) : base(index: index) { }

        public override TypeMappingDescriptor<Employee> BuildMapping(TypeMappingDescriptor<Employee> map) {
            return base.BuildMapping(map
                .Dynamic(false)
                .Properties(p => p
                    .SetupDefaults()
                    .Keyword(f => f.Name(e => e.Id).IndexName(Fields.Id))
                    .Keyword(f => f.Name(e => e.CompanyId).IndexName(Fields.CompanyId))
                    .Keyword(f => f.Name(e => e.CompanyName).IndexName(Fields.CompanyName))
                    .Text(f => f.Name(e => e.Name).IndexName(Fields.Name))
                    .Number(f => f.Name(e => e.Age).IndexName(Fields.Age))
                    .Number(f => f.Name(e => e.YearsEmployed).IndexName(Fields.YearsEmployed))
                ));
        }

        public class Fields {
            public const string Id = "id";
            public const string CompanyId = "company";
            public const string CompanyName = "company_name";
            public const string Name = "name";
            public const string Age = "age";
            public const string YearsEmployed = "years_employed";
        }
    }

    public class DailyEmployeeType : DailyIndexType<Employee> {
        public DailyEmployeeType(IIndex index) : base(index: index) { }

        public override TypeMappingDescriptor<Employee> BuildMapping(TypeMappingDescriptor<Employee> map) {
            return base.BuildMapping(map
                .Dynamic(false)
                .Properties(p => p
                    .SetupDefaults()
                    .Keyword(f => f.Name(e => e.Id).IndexName(Fields.Id))
                    .Keyword(f => f.Name(e => e.CompanyId).IndexName(Fields.CompanyId))
                    .Keyword(f => f.Name(e => e.CompanyName).IndexName(Fields.CompanyName))
                    .Text(f => f.Name(e => e.Name).IndexName(Fields.Name))
                    .Number(f => f.Name(e => e.Age).IndexName(Fields.Age))
                ));
        }

        public class Fields {
            public const string Id = "id";
            public const string CompanyId = "company";
            public const string CompanyName = "company_name";
            public const string Name = "name";
            public const string Age = "age";
        }
    }

    public class MonthlyEmployeeType : MonthlyIndexType<Employee> {
        public MonthlyEmployeeType(IIndex index) : base(index: index) { }

        public override TypeMappingDescriptor<Employee> BuildMapping(TypeMappingDescriptor<Employee> map) {
            return base.BuildMapping(map
                .Dynamic(false)
                .Properties(p => p
                    .SetupDefaults()
                    .Keyword(f => f.Name(e => e.Id).IndexName(Fields.Id))
                    .Keyword(f => f.Name(e => e.CompanyId).IndexName(Fields.CompanyId))
                    .Keyword(f => f.Name(e => e.CompanyName).IndexName(Fields.CompanyName))
                    .Text(f => f.Name(e => e.Name).IndexName(Fields.Name))
                    .Number(f => f.Name(e => e.Age).IndexName(Fields.Age))
                ));
        }

        public class Fields {
            public const string Id = "id";
            public const string CompanyId = "company";
            public const string CompanyName = "company_name";
            public const string Name = "name";
            public const string Age = "age";
        }
    }
}