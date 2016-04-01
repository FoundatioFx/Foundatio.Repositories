using System;
using System.Collections.Generic;
using Foundatio.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Tests.Models;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.Tests.Configuration {
    public class EmployeeIndex : IElasticIndex {
        public int Version => 1;
        public static string Alias => "employees";
        public string AliasName => Alias;
        public string VersionedName => String.Concat(AliasName, "-v", Version);

        public IDictionary<Type, IndexType> GetIndexTypes() {
            return new Dictionary<Type, IndexType> {
                { typeof(Employee), new IndexType { Name = "employee" } }
            };
        }

        public CreateIndexDescriptor CreateIndex(CreateIndexDescriptor idx) {
            return idx.Index(VersionedName).Mappings(maps => maps.Map<Employee>(GetEmployeeMap));
        }

        private ITypeMapping GetEmployeeMap(TypeMappingDescriptor<Employee> map) {
            return map
                .Dynamic()
                .Properties(p => p
                    .String(f => f.Name(e => e.Id).NotAnalyzed())
                    .String(f => f.Name(Fields.Employee.CompanyId).NotAnalyzed())
                    .String(f => f.Name(e => e.CompanyId).CopyTo(c => c.Fields(Fields.Employee.CompanyId)).Index(FieldIndexOption.No))
                    .Object<object>(o => o.Name("company").Properties(c =>
                        c.String(f => f.Name("name").NotAnalyzed())))
                    .String(f => f.Name(e => e.CompanyName).CopyTo(c => c.Fields(Fields.Employee.CompanyName)).Index(FieldIndexOption.No))
                    .String(f => f.Name(e => e.Name).NotAnalyzed())
                    .Number(f => f.Name(e => e.Age))
                    .Date(f => f.Name(Fields.Employee.CreatedUtc))
                    .Date(f => f.Name(e => e.CreatedUtc).CopyTo(c => c.Fields(Fields.Employee.CreatedUtc)).Index(NonStringIndexOption.No))
                    .Date(f => f.Name(Fields.Employee.UpdatedUtc))
                    .Date(f => f.Name(e => e.UpdatedUtc).CopyTo(c => c.Fields(Fields.Employee.UpdatedUtc)).Index(NonStringIndexOption.No))
                );
        }
        
        public class Fields {
            public class Employee {
                public const string Id = "id";
                public const string CompanyId = "company";
                public const string CompanyName = "company.name";
                public const string Name = "name";
                public const string Age = "age";
                public const string CreatedUtc = "created";
                public const string UpdatedUtc = "updated";
            }
        }
    }
}