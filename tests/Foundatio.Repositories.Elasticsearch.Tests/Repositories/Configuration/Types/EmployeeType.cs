using System;
using System.Threading.Tasks;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Tests.Repositories.Configuration.Types {
    public class EmployeeType : IndexTypeBase<Employee> {
        public EmployeeType(IIndex index) : base(index) { }

        public override PutMappingDescriptor<Employee> BuildMapping(PutMappingDescriptor<Employee> map) {
            return base.BuildMapping(map
                .Dynamic(false)
                .TimestampField(ts => ts.Enabled().Path(u => u.UpdatedUtc).IgnoreMissing(false))
                .Properties(p => p
                    .SetupDefaults()
                    .String(f => f.Name(e => e.Id).IndexName(Fields.Id).Index(FieldIndexOption.NotAnalyzed))
                    .String(f => f.Name(e => e.CompanyId).IndexName(Fields.CompanyId).Index(FieldIndexOption.NotAnalyzed))
                    .String(f => f.Name(e => e.CompanyName).IndexName(Fields.CompanyName).Index(FieldIndexOption.NotAnalyzed))
                    .String(f => f.Name(e => e.Name).IndexName(Fields.Name).Index(FieldIndexOption.Analyzed))
                    .GeoPoint(f => f.Name(e => e.Location))
                    .Number(f => f.Name(e => e.Age).IndexName(Fields.Age))
                ));
        }

        protected override void ConfigureQueryBuilder(ElasticQueryBuilder builder) {
            var aliasMap = new AliasMap {
                { "aliasedage", "age" }
            };

            builder.UseQueryParser(this, c => c
                .UseIncludes(i => ResolveInclude(i))
                .UseAliases(aliasMap)
            );
        }

        private async Task<string> ResolveInclude(string name) {
            await Task.Delay(100);
            return "aliasedage:10";
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

        public override PutMappingDescriptor<Employee> BuildMapping(PutMappingDescriptor<Employee> map) {
            return base.BuildMapping(map
                .Dynamic(false)
                .TimestampField(ts => ts.Enabled().Path(u => u.UpdatedUtc).IgnoreMissing(false))
                .Properties(p => p
                    .SetupDefaults()
                    .String(f => f.Name(e => e.Id).IndexName(Fields.Id).Index(FieldIndexOption.NotAnalyzed))
                    .String(f => f.Name(e => e.CompanyId).IndexName(Fields.CompanyId).Index(FieldIndexOption.NotAnalyzed))
                    .String(f => f.Name(e => e.CompanyName).IndexName(Fields.CompanyName).Index(FieldIndexOption.NotAnalyzed))
                    .String(f => f.Name(e => e.Name).IndexName(Fields.Name).Index(FieldIndexOption.Analyzed))
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

        public override PutMappingDescriptor<Employee> BuildMapping(PutMappingDescriptor<Employee> map) {
            return base.BuildMapping(map
                .Dynamic(false)
                .TimestampField(ts => ts.Enabled().Path(u => u.UpdatedUtc).IgnoreMissing(false))
                .Properties(p => p
                    .SetupDefaults()
                    .String(f => f.Name(e => e.Id).IndexName(Fields.Id).Index(FieldIndexOption.NotAnalyzed))
                    .String(f => f.Name(e => e.CompanyId).IndexName(Fields.CompanyId).Index(FieldIndexOption.NotAnalyzed))
                    .String(f => f.Name(e => e.CompanyName).IndexName(Fields.CompanyName).Index(FieldIndexOption.NotAnalyzed))
                    .String(f => f.Name(e => e.Name).IndexName(Fields.Name).Index(FieldIndexOption.Analyzed))
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

        public override PutMappingDescriptor<Employee> BuildMapping(PutMappingDescriptor<Employee> map) {
            return base.BuildMapping(map
                .Dynamic(false)
                .TimestampField(ts => ts.Enabled().Path(u => u.UpdatedUtc).IgnoreMissing(false))
                .Properties(p => p
                    .SetupDefaults()
                    .String(f => f.Name(e => e.Id).IndexName(Fields.Id).Index(FieldIndexOption.NotAnalyzed))
                    .String(f => f.Name(e => e.CompanyId).IndexName(Fields.CompanyId).Index(FieldIndexOption.NotAnalyzed))
                    .String(f => f.Name(e => e.CompanyName).IndexName(Fields.CompanyName).Index(FieldIndexOption.NotAnalyzed))
                    .String(f => f.Name(e => e.Name).IndexName(Fields.Name).Index(FieldIndexOption.Analyzed))
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