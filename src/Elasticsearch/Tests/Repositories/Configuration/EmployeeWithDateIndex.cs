using System;
using System.Collections.Generic;
using Foundatio.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Models;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Tests.Configuration {
    public class EmployeeWithDateIndex : ITemplatedElasticIndex {
        public int Version => 1;
        public static string Alias => "employees_with_date";
        public string AliasName => Alias;
        public string VersionedName => String.Concat(AliasName, "-v", Version);

        public IDictionary<Type, IndexType> GetIndexTypes() {
            return new Dictionary<Type, IndexType> {
                { typeof(EmployeeWithDate), new IndexType { Name = "employees_with_date" } }
            };
        }

        public CreateIndexDescriptor CreateIndex(CreateIndexDescriptor idx) {
            throw new NotImplementedException();
        }

        public PutIndexTemplateDescriptor CreateTemplate(PutIndexTemplateDescriptor template) {
            return template
                .Template(VersionedName + "-*")
                .Mappings(maps => maps
                    .Map<EmployeeWithDate>(map => map
                        .Dynamic()
                        .TimestampField(ts => ts.Enabled().Path(u => u.UpdatedUtc).IgnoreMissing(false))
                        .Properties(p => p
                            .String(f => f.Name(e => e.Id).IndexName(Fields.EmployeeWithDate.Id).NotAnalyzed())
                            .String(f => f.IndexName(Fields.EmployeeWithDate.CompanyId).NotAnalyzed())
                            .String(f => f.Name(e => e.CompanyId).CopyTo(c => c.Fields(Fields.EmployeeWithDate.CompanyId)).Index(FieldIndexOption.No))
                            .String(f => f.IndexName(Fields.EmployeeWithDate.CompanyName).NotAnalyzed())
                            .String(f => f.Name(e => e.CompanyName).CopyTo(c => c.Fields(Fields.EmployeeWithDate.CompanyName)).Index(FieldIndexOption.No))
                            .String(f => f.Name(e => e.Name).IndexName(Fields.EmployeeWithDate.Name).NotAnalyzed())
                            .Number(f => f.Name(e => e.Age).IndexName(Fields.EmployeeWithDate.Age))
                            .Date(f => f.IndexName(Fields.EmployeeWithDate.StartDate))
                            .Date(f => f.Name(e => e.StartDate).CopyTo(c => c.Fields(Fields.EmployeeWithDate.StartDate)).Index(NonStringIndexOption.No))
                            .Date(f => f.IndexName(Fields.EmployeeWithDate.CreatedUtc))
                            .Date(f => f.Name(e => e.CreatedUtc).CopyTo(c => c.Fields(Fields.EmployeeWithDate.CreatedUtc)).Index(NonStringIndexOption.No))
                            .Date(f => f.IndexName(Fields.EmployeeWithDate.UpdatedUtc))
                            .Date(f => f.Name(e => e.UpdatedUtc).CopyTo(c => c.Fields(Fields.EmployeeWithDate.UpdatedUtc)).Index(NonStringIndexOption.No))
                        )));
        }
        
        public class Fields {
            public class EmployeeWithDate {
                public const string Id = "id";
                public const string CompanyId = "company";
                public const string CompanyName = "company.name";
                public const string Name = "name";
                public const string Age = "age";
                public const string StartDate = "start";
                public const string CreatedUtc = "created";
                public const string UpdatedUtc = "updated";
            }
        }
    }
}