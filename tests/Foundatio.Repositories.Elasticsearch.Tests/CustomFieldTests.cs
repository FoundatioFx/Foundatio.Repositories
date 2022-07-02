using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Repositories.Elasticsearch.CustomFields;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Exceptions;
using Microsoft.Extensions.Logging;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class CustomFieldTests : ElasticRepositoryTestBase {
    private readonly ICustomFieldDefinitionRepository _customFieldDefinitionRepository;
    private readonly IEmployeeRepository _employeeRepository;

    public CustomFieldTests(ITestOutputHelper output) : base(output) {
        _customFieldDefinitionRepository = _configuration.CustomFieldDefinitionRepository;
        _employeeRepository = new EmployeeRepository(_configuration);
    }

    public override async Task InitializeAsync() {
        await base.InitializeAsync();
        await RemoveDataAsync();
    }

    [Fact]
    public async Task WillValidate() {
        await Assert.ThrowsAsync<DocumentValidationException>(() => _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition {
            //EntityType = "Employee",
            TenantKey = "1",
            Name = "MyField1",
            IndexType = "string"
        }));

        await Assert.ThrowsAsync<DocumentValidationException>(() => _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition {
            EntityType = "Employee",
            //TenantKey = "1",
            Name = "MyField1",
            IndexType = "string"
        }));

        await Assert.ThrowsAsync<DocumentValidationException>(() => _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition {
            EntityType = "Employee",
            TenantKey = "1",
            //Name = "MyField1",
            IndexType = "string"
        }));

        await Assert.ThrowsAsync<DocumentValidationException>(() => _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition {
            EntityType = "Employee",
            TenantKey = "1",
            Name = "MyField1",
            //IndexType = "string"
        }));

        await Assert.ThrowsAsync<DocumentValidationException>(() => _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition {
            EntityType = "Employee",
            TenantKey = "1",
            Name = "MyField1",
            IndexType = "string",
            IndexSlot = 1
        }));
    }

    [Fact]
    public async Task CanAddNewFieldsAndReserveSlots() {
        var customField = await _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition {
            EntityType = "Employee",
            TenantKey = "1",
            Name = "MyField1",
            IndexType = "string"
        });
        Assert.Equal(1, customField.IndexSlot);
        var mapping = await _customFieldDefinitionRepository.GetFieldMappingAsync("Employee", "1");
        Assert.Contains(mapping.Keys, c => c == "MyField1");

        customField = await _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition {
            EntityType = "Employee",
            TenantKey = "1",
            Name = "MyField2",
            IndexType = "string"
        });
        Assert.Equal(2, customField.IndexSlot);
        mapping = await _customFieldDefinitionRepository.GetFieldMappingAsync("Employee", "1");
        Assert.Contains(mapping.Keys, c => c == "MyField2");

        customField = await _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition {
            EntityType = "Employee",
            TenantKey = "1",
            Name = "MyField3",
            IndexType = "string"
        });
        Assert.Equal(3, customField.IndexSlot);
        mapping = await _customFieldDefinitionRepository.GetFieldMappingAsync("Employee", "1");
        Assert.Contains(mapping.Keys, c => c == "MyField3");
    }

    [Fact]
    public async Task CanUseDeletedSlotAndName() {
        var customField = await _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition {
            EntityType = "Employee",
            TenantKey = "1",
            Name = "MyField1",
            IndexType = "string"
        });
        Assert.Equal(1, customField.IndexSlot);
        var mapping = await _customFieldDefinitionRepository.GetFieldMappingAsync("Employee", "1");
        Assert.Contains(mapping.Keys, c => c == "MyField1");

        var customField2 = await _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition {
            EntityType = "Employee",
            TenantKey = "1",
            Name = "MyField2",
            IndexType = "string"
        });
        Assert.Equal(2, customField2.IndexSlot);
        mapping = await _customFieldDefinitionRepository.GetFieldMappingAsync("Employee", "1");
        Assert.Contains(mapping.Keys, c => c == "MyField2");

        customField = await _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition {
            EntityType = "Employee",
            TenantKey = "1",
            Name = "MyField3",
            IndexType = "string"
        });
        Assert.Equal(3, customField.IndexSlot);
        mapping = await _customFieldDefinitionRepository.GetFieldMappingAsync("Employee", "1");
        Assert.Contains(mapping.Keys, c => c == "MyField3");

        customField2.IsDeleted = true;
        await _customFieldDefinitionRepository.SaveAsync(customField2);
        mapping = await _customFieldDefinitionRepository.GetFieldMappingAsync("Employee", "1");
        Assert.DoesNotContain(mapping.Keys, c => c == "MyField2");

        customField = await _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition {
            EntityType = "Employee",
            TenantKey = "1",
            Name = "MyField4",
            IndexType = "string"
        });
        Assert.Equal(4, customField.IndexSlot);
        mapping = await _customFieldDefinitionRepository.GetFieldMappingAsync("Employee", "1");
        Assert.Contains(mapping.Keys, c => c == "MyField4");

        await _customFieldDefinitionRepository.RemoveAsync(customField2);

        customField = await _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition {
            EntityType = "Employee",
            TenantKey = "1",
            Name = "MyField2",
            IndexType = "string"
        });
        Assert.Equal(2, customField.IndexSlot);
        mapping = await _customFieldDefinitionRepository.GetFieldMappingAsync("Employee", "1");
        Assert.Contains(mapping.Keys, c => c == "MyField2");
    }

    [Fact]
    public async Task CanAddNewFieldsAndReserveSlotsConcurrently() {
        Log.SetLogLevel<CustomFieldDefinitionRepository>(LogLevel.Trace);

        const int COUNT = 100;
        await Parallel.ForEachAsync(Enumerable.Range(1, COUNT), new ParallelOptions { MaxDegreeOfParallelism = 2 }, async (index, ct) => {
            var customField = await _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition {
                EntityType = "Employee",
                TenantKey = "1",
                Name = "MyField" + index,
                IndexType = "string"
            });

            Assert.NotNull(customField);
            Assert.InRange(customField.IndexSlot, 1, COUNT);
        });

        var usedSlots = new HashSet<int>();
        var customFields = await _customFieldDefinitionRepository.GetAllAsync(o => o.PageLimit(1000));
        foreach (var doc in customFields.Documents) {
            Assert.DoesNotContain(doc.IndexSlot, usedSlots);
            usedSlots.Add(doc.IndexSlot);
        }

        Assert.Equal(COUNT, usedSlots.Count);
    }

    [Fact]
    public async Task CanAddNewFieldsAndReserveSlotsConcurrentlyAcrossTenantsAndFieldTypes() {
        Log.SetLogLevel<CustomFieldDefinitionRepository>(LogLevel.Information);

        const int COUNT = 1000;
        await Parallel.ForEachAsync(Enumerable.Range(1, COUNT), new ParallelOptions { MaxDegreeOfParallelism = 2 }, async (index, ct) => {
            var customField = await _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition {
                EntityType = "Employee",
                TenantKey = index % 2 == 1 ? "1" : "2",
                Name = "MyField" + index,
                IndexType = index % 2 == 1 ? "number" : "string"
            });

            Assert.NotNull(customField);
            Assert.InRange(customField.IndexSlot, 1, COUNT);
        });

        var customFields = await _customFieldDefinitionRepository.GetAllAsync(o => o.PageLimit(1000));
        var fieldGroups = customFields.Documents.GroupBy(cf => (cf.TenantKey, cf.IndexType));

        foreach (var fieldGroup in fieldGroups) {
            var usedSlots = new List<int>();
            foreach (var doc in fieldGroup) {
                if (usedSlots.Contains(doc.IndexSlot))
                    throw new ApplicationException($"Found duplicate slot {doc.IndexSlot} in {doc.TenantKey}:{doc.IndexType}");
                usedSlots.Add(doc.IndexSlot);
            }
        }
    }

    [Fact]
    public async Task CanSearchByCustomField() {
        var customField = await _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition {
            EntityType = "Employee",
            TenantKey = "1",
            Name = "MyField1",
            IndexType = "string"
        });
        Assert.Equal(1, customField.IndexSlot);

        var employee = EmployeeGenerator.Generate(age: 19);
        employee.CompanyId = "1";
        employee.PhoneNumbers.Add(new PhoneInfo { Number = "214-222-2222" });
        employee.Data["MyField1"] = "hey";
        await _employeeRepository.AddAsync(employee, o => o.ImmediateConsistency());

        var results = await _employeeRepository.FindAsync(q => q.Company("1").FilterExpression("myfield1:hey"), o => o.QueryLogLevel(LogLevel.Information));
        var employees = results.Documents.ToArray();
        Assert.Single(employees);
        Assert.Equal(19, employees[0].Age);
        Assert.Single(employees[0].Data);
        Assert.Equal("hey", employees[0].Data["MyField1"]);
    }

    [Fact]
    public async Task CanAutoCreateUnmappedCustomField() {
        var fieldMapping = await _customFieldDefinitionRepository.GetFieldMappingAsync("Employee", "1");
        Assert.DoesNotContain(fieldMapping, m => m.Key == "MyField1");

        var employee = EmployeeGenerator.Generate(age: 19);
        employee.CompanyId = "1";
        employee.PhoneNumbers.Add(new PhoneInfo { Number = "214-222-2222" });
        employee.Data["MyField1"] = "hey";
        await _employeeRepository.AddAsync(employee, o => o.ImmediateConsistency());

        fieldMapping = await _customFieldDefinitionRepository.GetFieldMappingAsync("Employee", "1");
        Assert.Contains(fieldMapping, m => m.Key == "MyField1");

        var results = await _employeeRepository.FindAsync(q => q.Company("1").FilterExpression("myfield1:hey"), o => o.QueryLogLevel(LogLevel.Information));
        var employees = results.Documents.ToArray();
        Assert.Single(employees);
        Assert.Equal(19, employees[0].Age);
        Assert.Single(employees[0].Data);
        Assert.Equal("hey", employees[0].Data["MyField1"]);
    }

    [Fact]
    public async Task CanHandleWrongFieldValueType() {
        var customField = await _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition {
            EntityType = "Employee",
            TenantKey = "1",
            Name = "MyField1",
            IndexType = IntegerFieldType.IndexType
        });
        Assert.Equal(1, customField.IndexSlot);

        var employee = EmployeeGenerator.Generate(age: 19);
        employee.CompanyId = "1";
        employee.PhoneNumbers.Add(new PhoneInfo { Number = "214-222-2222" });
        employee.Data["MyField1"] = "hey";
        await _employeeRepository.AddAsync(employee, o => o.ImmediateConsistency());

        var results = await _employeeRepository.FindAsync(q => q.Company("1").FilterExpression("_exists_:myfield1"), o => o.QueryLogLevel(LogLevel.Information));
        var employees = results.Documents.ToArray();
        Assert.Empty(employees);
    }

    // Remove idx data when removing custom field
    // Move idx data when changing field type
}
