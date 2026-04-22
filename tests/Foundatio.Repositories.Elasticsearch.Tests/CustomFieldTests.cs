using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Caching;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.CustomFields;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
using Foundatio.Repositories.Exceptions;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class CustomFieldTests : ElasticRepositoryTestBase
{
    private readonly ICustomFieldDefinitionRepository _customFieldDefinitionRepository;
    private readonly IEmployeeWithCustomFieldsRepository _employeeRepository;
    private readonly InMemoryCacheClient _repocache;

    public CustomFieldTests(ITestOutputHelper output) : base(output)
    {
        Assert.NotNull(_configuration.CustomFieldDefinitionRepository);
        _customFieldDefinitionRepository = _configuration.CustomFieldDefinitionRepository;
        _employeeRepository = new EmployeeWithCustomFieldsRepository(_configuration);
        _repocache = Assert.IsType<InMemoryCacheClient>(_configuration.Cache);
    }

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        await RemoveDataAsync();
    }

    [Fact]
    public async Task WillValidate()
    {
        await Assert.ThrowsAsync<DocumentValidationException>(() => _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition
        {
            //EntityType = nameof(EmployeeWithCustomFields),
            TenantKey = "1",
            Name = "MyField1",
            IndexType = "string"
        }));

        await Assert.ThrowsAsync<DocumentValidationException>(() => _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition
        {
            EntityType = nameof(EmployeeWithCustomFields),
            //TenantKey = "1",
            Name = "MyField1",
            IndexType = "string"
        }));

        await Assert.ThrowsAsync<DocumentValidationException>(() => _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition
        {
            EntityType = nameof(EmployeeWithCustomFields),
            TenantKey = "1",
            //Name = "MyField1",
            IndexType = "string"
        }));

        await Assert.ThrowsAsync<DocumentValidationException>(() => _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition
        {
            EntityType = nameof(EmployeeWithCustomFields),
            TenantKey = "1",
            Name = "MyField1",
            //IndexType = "string"
        }));

        await Assert.ThrowsAsync<DocumentValidationException>(() => _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition
        {
            EntityType = nameof(EmployeeWithCustomFields),
            TenantKey = "1",
            Name = "MyField1",
            IndexType = "string",
            IndexSlot = 1
        }));
    }

    [Fact]
    public async Task CanAddNewFieldsAndReserveSlots()
    {
        var customField = await _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition
        {
            EntityType = nameof(EmployeeWithCustomFields),
            TenantKey = "1",
            Name = "MyField1",
            IndexType = "string"
        });
        Assert.Equal(1, customField.IndexSlot);
        Assert.Equal(2, _repocache.Count);
        var mapping = await _customFieldDefinitionRepository.GetFieldMappingAsync(nameof(EmployeeWithCustomFields), "1");
        Assert.Contains(mapping.Keys, c => c == "MyField1");
        await _customFieldDefinitionRepository.GetFieldMappingAsync(nameof(EmployeeWithCustomFields), "1");
        Assert.Equal(1, _repocache.Hits);

        customField = await _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition
        {
            EntityType = nameof(EmployeeWithCustomFields),
            TenantKey = "1",
            Name = "MyField2",
            IndexType = "string"
        });
        Assert.Equal(2, customField.IndexSlot);
        mapping = await _customFieldDefinitionRepository.GetFieldMappingAsync(nameof(EmployeeWithCustomFields), "1");
        Assert.Contains(mapping.Keys, c => c == "MyField2");
        Assert.Equal(3, _repocache.Hits);

        customField = await _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition
        {
            EntityType = nameof(EmployeeWithCustomFields),
            TenantKey = "1",
            Name = "MyField3",
            IndexType = "string"
        });
        Assert.Equal(3, customField.IndexSlot);
        mapping = await _customFieldDefinitionRepository.GetFieldMappingAsync(nameof(EmployeeWithCustomFields), "1");
        Assert.Contains(mapping.Keys, c => c == "MyField3");
        Assert.Equal(5, _repocache.Hits);
    }

    [Fact]
    public async Task WontAllowSameFieldNameWithDifferentType()
    {
        var customField = await _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition
        {
            EntityType = nameof(EmployeeWithCustomFields),
            TenantKey = "1",
            Name = "MyField1",
            IndexType = StringFieldType.IndexType
        });
        Assert.Equal(1, customField.IndexSlot);
        var mapping = await _customFieldDefinitionRepository.GetFieldMappingAsync(nameof(EmployeeWithCustomFields), "1");
        Assert.Contains(mapping.Keys, c => c == "MyField1");

        await Assert.ThrowsAsync<DocumentValidationException>(() => _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition
        {
            EntityType = nameof(EmployeeWithCustomFields),
            TenantKey = "1",
            Name = "MyField1",
            IndexType = IntegerFieldType.IndexType
        }));
    }

    [Fact]
    public async Task CanUseDeletedSlotAndName()
    {
        var customField = await _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition
        {
            EntityType = nameof(EmployeeWithCustomFields),
            TenantKey = "1",
            Name = "MyField1",
            IndexType = "string"
        });
        Assert.Equal(1, customField.IndexSlot);
        var mapping = await _customFieldDefinitionRepository.GetFieldMappingAsync(nameof(EmployeeWithCustomFields), "1");
        Assert.Contains(mapping.Keys, c => c == "MyField1");

        var customField2 = await _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition
        {
            EntityType = nameof(EmployeeWithCustomFields),
            TenantKey = "1",
            Name = "MyField2",
            IndexType = "string"
        });
        Assert.Equal(2, customField2.IndexSlot);
        mapping = await _customFieldDefinitionRepository.GetFieldMappingAsync(nameof(EmployeeWithCustomFields), "1");
        Assert.Contains(mapping.Keys, c => c == "MyField2");

        customField = await _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition
        {
            EntityType = nameof(EmployeeWithCustomFields),
            TenantKey = "1",
            Name = "MyField3",
            IndexType = "string"
        });
        Assert.Equal(3, customField.IndexSlot);
        mapping = await _customFieldDefinitionRepository.GetFieldMappingAsync(nameof(EmployeeWithCustomFields), "1");
        Assert.Contains(mapping.Keys, c => c == "MyField3");

        customField2.IsDeleted = true;
        await _customFieldDefinitionRepository.SaveAsync(customField2);
        mapping = await _customFieldDefinitionRepository.GetFieldMappingAsync(nameof(EmployeeWithCustomFields), "1");
        Assert.DoesNotContain(mapping.Keys, c => c == "MyField2");

        var deletedFields = await _customFieldDefinitionRepository.FindAsync(q => q.FieldEquals(cf => cf.EntityType, nameof(EmployeeWithCustomFields)).FieldEquals(cf => cf.TenantKey, "1"), o => o.IncludeSoftDeletes().PageLimit(1000));
        Assert.Contains(deletedFields.Documents, d => d.Name == "MyField2");

        customField = await _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition
        {
            EntityType = nameof(EmployeeWithCustomFields),
            TenantKey = "1",
            Name = "MyField2",
            IndexType = "string"
        });
        Assert.Equal(4, customField.IndexSlot);
        mapping = await _customFieldDefinitionRepository.GetFieldMappingAsync(nameof(EmployeeWithCustomFields), "1");
        Assert.Contains(mapping.Keys, c => c == "MyField2");

        await _customFieldDefinitionRepository.RemoveAsync(customField2);

        customField = await _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition
        {
            EntityType = nameof(EmployeeWithCustomFields),
            TenantKey = "1",
            Name = "MyField4",
            IndexType = "string"
        });
        Assert.Equal(2, customField.IndexSlot);
        mapping = await _customFieldDefinitionRepository.GetFieldMappingAsync(nameof(EmployeeWithCustomFields), "1");
        Assert.Contains(mapping.Keys, c => c == "MyField4");

        await _customFieldDefinitionRepository.RemoveAllAsync(q => q.FieldEquals(d => d.EntityType, nameof(EmployeeWithCustomFields)));

        customField = await _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition
        {
            EntityType = nameof(EmployeeWithCustomFields),
            TenantKey = "1",
            Name = "MyField2",
            IndexType = "string"
        });
        Assert.Equal(1, customField.IndexSlot);
        mapping = await _customFieldDefinitionRepository.GetFieldMappingAsync(nameof(EmployeeWithCustomFields), "1");
        Assert.Contains(mapping.Keys, c => c == "MyField2");
    }

    [Fact]
    public async Task CanAddNewFieldsAndReserveSlotsConcurrently()
    {
        Log.SetLogLevel<CustomFieldDefinitionRepository>(LogLevel.Trace);

        const int COUNT = 100;
        await Parallel.ForEachAsync(Enumerable.Range(1, COUNT), new ParallelOptions { MaxDegreeOfParallelism = 2 }, async (index, ct) =>
        {
            var customField = await _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition
            {
                EntityType = nameof(EmployeeWithCustomFields),
                TenantKey = "1",
                Name = "MyField" + index,
                IndexType = "string"
            });

            Assert.NotNull(customField);
            Assert.InRange(customField.IndexSlot, 1, COUNT);
        });

        var usedSlots = new HashSet<int>();
        var customFields = await _customFieldDefinitionRepository.GetAllAsync(o => o.PageLimit(1000));
        foreach (var doc in customFields.Documents)
        {
            Assert.DoesNotContain(doc.IndexSlot, usedSlots);
            usedSlots.Add(doc.IndexSlot);
        }

        Assert.Equal(COUNT, usedSlots.Count);
    }

    [Fact]
    public async Task CanAddNewFieldsAndReserveSlotsConcurrentlyAcrossTenantsAndFieldTypes()
    {
        Log.SetLogLevel<CustomFieldDefinitionRepository>(LogLevel.Information);

        const int COUNT = 1000;
        await Parallel.ForEachAsync(Enumerable.Range(1, COUNT), new ParallelOptions { MaxDegreeOfParallelism = 2 }, async (index, _) =>
        {
            var customField = await _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition
            {
                EntityType = nameof(EmployeeWithCustomFields),
                TenantKey = index % 2 == 1 ? "1" : "2",
                Name = "MyField" + index,
                IndexType = index % 2 == 1 ? "number" : "string"
            });

            Assert.NotNull(customField);
            Assert.InRange(customField.IndexSlot, 1, COUNT);
        });

        var customFields = await _customFieldDefinitionRepository.GetAllAsync(o => o.PageLimit(1000));
        var fieldGroups = customFields.Documents.GroupBy(cf => (cf.TenantKey, cf.IndexType));

        foreach (var fieldGroup in fieldGroups)
        {
            var usedSlots = new List<int>();
            foreach (var doc in fieldGroup)
            {
                if (usedSlots.Contains(doc.IndexSlot))
                    throw new ApplicationException($"Found duplicate slot {doc.IndexSlot} in {doc.TenantKey}:{doc.IndexType}");
                usedSlots.Add(doc.IndexSlot);
            }
        }
    }

    [Fact]
    public async Task CanSearchByCustomField()
    {
        var customField = await _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition
        {
            EntityType = nameof(EmployeeWithCustomFields),
            TenantKey = "1",
            Name = "MyField1",
            IndexType = "string"
        });
        Assert.Equal(1, customField.IndexSlot);

        var employee = EmployeeWithCustomFieldsGenerator.Generate(age: 19);
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
    public async Task CanAutoCreateUnmappedCustomField()
    {
        await _customFieldDefinitionRepository.AddAsync([
            new CustomFieldDefinition
            {
                EntityType = nameof(EmployeeWithCustomFields),
                TenantKey = "1",
                Name = "Field1",
                IndexType = IntegerFieldType.IndexType
            },
            new CustomFieldDefinition
            {
                EntityType = nameof(EmployeeWithCustomFields),
                TenantKey = "1",
                Name = "Field2",
                IndexType = IntegerFieldType.IndexType
            },
            new CustomFieldDefinition
            {
                EntityType = nameof(EmployeeWithCustomFields),
                TenantKey = "1",
                Name = "Calculated",
                IndexType = IntegerFieldType.IndexType,
                ProcessMode = CustomFieldProcessMode.AlwaysProcess,
                Data = new Dictionary<string, object?> { { "Expression", "(source.Data.Field1 || 0) + (source.Data.Field2 || 0)" } }
            }
        ]);

        var fieldMapping = await _customFieldDefinitionRepository.GetFieldMappingAsync(nameof(EmployeeWithCustomFields), "1");
        Assert.DoesNotContain(fieldMapping, m => m.Key == "MyField1");

        var employee1 = EmployeeWithCustomFieldsGenerator.Generate(age: 19);
        employee1.CompanyId = "1";
        employee1.PhoneNumbers.Add(new PhoneInfo { Number = "214-222-2222" });
        employee1.Data["MyField1"] = "hey1";
        employee1.Data["Calculated"] = 1;
        var employee2 = EmployeeWithCustomFieldsGenerator.Generate(age: 21);
        employee2.CompanyId = "1";
        employee2.PhoneNumbers.Add(new PhoneInfo { Number = "214-111-1111" });
        employee2.Data["myfield1"] = "hey2";
        await _employeeRepository.AddAsync([employee1, employee2], o => o.ImmediateConsistency());

        fieldMapping = await _customFieldDefinitionRepository.GetFieldMappingAsync(nameof(EmployeeWithCustomFields), "1");
        Assert.Contains(fieldMapping, m => m.Key == "MyField1");

        const int count = 2;
        await Parallel.ForEachAsync(Enumerable.Range(1, count), new ParallelOptions { MaxDegreeOfParallelism = 2 }, async (index, ct) =>
        {
            var randomEmployees = EmployeeWithCustomFieldsGenerator.GenerateEmployees(count: 5, companyId: "1");
            randomEmployees.ForEach(e =>
            {
                for (int i = 0; i < 5; i++)
                    e.Data["Field" + i] = i;
            });
            await _employeeRepository.AddAsync(randomEmployees, o => o.ImmediateConsistency());
        });

        fieldMapping = await _customFieldDefinitionRepository.GetFieldMappingAsync(nameof(EmployeeWithCustomFields), "1");

        var results = await _employeeRepository.FindAsync(q => q.Company("1").FilterExpression("myfield1:hey1"), o => o.QueryLogLevel(LogLevel.Information));
        var employees = results.Documents.ToArray();
        Assert.Single(employees);
        Assert.Equal(19, employees[0].Age);
        Assert.Equal(2, employees[0].Data.Count);
        Assert.Equal("hey1", employees[0].Data["MyField1"]);

        results = await _employeeRepository.FindAsync(q => q.Company("1").FilterExpression("myfield1:hey2"), o => o.QueryLogLevel(LogLevel.Information));
        employees = results.Documents.ToArray();
        Assert.Single(employees);
        Assert.Equal(21, employees[0].Age);
        Assert.Equal(2, employees[0].Data.Count);
        Assert.Equal("hey2", employees[0].Data["myfield1"]);
    }

    [Fact]
    public async Task FieldHasValue_WithCustomFieldName_ResolvesToIndexSlot()
    {
        // Arrange
        await _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition
        {
            EntityType = nameof(EmployeeWithCustomFields),
            TenantKey = "1",
            Name = "MyField1",
            IndexType = StringFieldType.IndexType
        });

        var withField = EmployeeWithCustomFieldsGenerator.Generate(age: 19);
        withField.CompanyId = "1";
        withField.Data["MyField1"] = "hey";
        var withoutField = EmployeeWithCustomFieldsGenerator.Generate(age: 25);
        withoutField.CompanyId = "1";
        await _employeeRepository.AddAsync([withField, withoutField], o => o.ImmediateConsistency());

        // Act
        var results = await _employeeRepository.FindAsync(q => q.Company("1").FieldHasValue("myfield1"));

        // Assert
        Assert.Single(results.Documents);
        Assert.Equal(19, results.Documents.First().Age);
    }

    [Fact]
    public async Task FieldEquals_WithCustomFieldName_ResolvesToIndexSlot()
    {
        // Arrange
        await _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition
        {
            EntityType = nameof(EmployeeWithCustomFields),
            TenantKey = "1",
            Name = "MyField1",
            IndexType = StringFieldType.IndexType
        });

        var single = EmployeeWithCustomFieldsGenerator.Generate(age: 19);
        single.CompanyId = "1";
        single.Data["MyField1"] = "hello";
        var multi = EmployeeWithCustomFieldsGenerator.Generate(age: 25);
        multi.CompanyId = "1";
        multi.Data["MyField1"] = "hello world";
        var other = EmployeeWithCustomFieldsGenerator.Generate(age: 30);
        other.CompanyId = "1";
        other.Data["MyField1"] = "other";
        await _employeeRepository.AddAsync([single, multi, other], o => o.ImmediateConsistency());

        // Act — single-word exact match must NOT match "hello world"
        var results = await _employeeRepository.FindAsync(q => q.Company("1").FieldEquals("myfield1", "hello"));
        Assert.Single(results.Documents);
        Assert.Equal(19, results.Documents.First().Age);

        // Act — multi-word exact match must NOT match "hello"
        results = await _employeeRepository.FindAsync(q => q.Company("1").FieldEquals("myfield1", "hello world"));
        Assert.Single(results.Documents);
        Assert.Equal(25, results.Documents.First().Age);
    }

    [Fact]
    public async Task FieldNotEquals_WithCustomFieldName_ResolvesToIndexSlot()
    {
        // Arrange
        await _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition
        {
            EntityType = nameof(EmployeeWithCustomFields),
            TenantKey = "1",
            Name = "MyField1",
            IndexType = StringFieldType.IndexType
        });

        var single = EmployeeWithCustomFieldsGenerator.Generate(age: 19);
        single.CompanyId = "1";
        single.Data["MyField1"] = "hello";
        var multi = EmployeeWithCustomFieldsGenerator.Generate(age: 25);
        multi.CompanyId = "1";
        multi.Data["MyField1"] = "hello world";
        var other = EmployeeWithCustomFieldsGenerator.Generate(age: 30);
        other.CompanyId = "1";
        other.Data["MyField1"] = "other";
        await _employeeRepository.AddAsync([single, multi, other], o => o.ImmediateConsistency());

        // Act — excluding "hello" must return both "hello world" and "other"
        var results = await _employeeRepository.FindAsync(q => q.Company("1").FieldNotEquals("myfield1", "hello"));
        Assert.Equal(2, results.Documents.Count);
        Assert.DoesNotContain(results.Documents, d => d.Age == 19);

        // Act — excluding "hello world" must return both "hello" and "other"
        results = await _employeeRepository.FindAsync(q => q.Company("1").FieldNotEquals("myfield1", "hello world"));
        Assert.Equal(2, results.Documents.Count);
        Assert.DoesNotContain(results.Documents, d => d.Age == 25);
    }

    [Fact]
    public async Task FieldEmpty_WithCustomFieldName_ResolvesToIndexSlot()
    {
        // Arrange
        await _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition
        {
            EntityType = nameof(EmployeeWithCustomFields),
            TenantKey = "1",
            Name = "MyField1",
            IndexType = StringFieldType.IndexType
        });

        var withField = EmployeeWithCustomFieldsGenerator.Generate(age: 19);
        withField.CompanyId = "1";
        withField.Data["MyField1"] = "hey";
        var withoutField = EmployeeWithCustomFieldsGenerator.Generate(age: 25);
        withoutField.CompanyId = "1";
        await _employeeRepository.AddAsync([withField, withoutField], o => o.ImmediateConsistency());

        // Act
        var results = await _employeeRepository.FindAsync(q => q.Company("1").FieldEmpty("myfield1"));

        // Assert
        Assert.Single(results.Documents);
        Assert.Equal(25, results.Documents.First().Age);
    }

    [Fact]
    public async Task FieldContains_WithCustomFieldName_ResolvesToIndexSlot()
    {
        // Arrange
        await _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition
        {
            EntityType = nameof(EmployeeWithCustomFields),
            TenantKey = "1",
            Name = "MyField1",
            IndexType = StringFieldType.IndexType
        });

        var single = EmployeeWithCustomFieldsGenerator.Generate(age: 19);
        single.CompanyId = "1";
        single.Data["MyField1"] = "hello";
        var multi = EmployeeWithCustomFieldsGenerator.Generate(age: 25);
        multi.CompanyId = "1";
        multi.Data["MyField1"] = "hello world";
        var other = EmployeeWithCustomFieldsGenerator.Generate(age: 30);
        other.CompanyId = "1";
        other.Data["MyField1"] = "other";
        await _employeeRepository.AddAsync([single, multi, other], o => o.ImmediateConsistency());

        // Act — "hello" single-token match hits both "hello" and "hello world"
        var results = await _employeeRepository.FindAsync(q => q.Company("1").FieldCondition("myfield1", ComparisonOperator.Contains, "hello"));
        Assert.Equal(2, results.Documents.Count);
        Assert.DoesNotContain(results.Documents, d => d.Age == 30);

        // Act — "hello world" requires BOTH tokens, matches only "hello world" (not "hello" alone)
        results = await _employeeRepository.FindAsync(q => q.Company("1").FieldCondition("myfield1", ComparisonOperator.Contains, "hello world"));
        Assert.Single(results.Documents);
        Assert.Equal(25, results.Documents.First().Age);

        // Act — "world hello" reversed token order still matches "hello world" (AND is order-independent)
        results = await _employeeRepository.FindAsync(q => q.Company("1").FieldCondition("myfield1", ComparisonOperator.Contains, "world hello"));
        Assert.Single(results.Documents);
        Assert.Equal(25, results.Documents.First().Age);

        // Act — "other" single-token match hits only "other"
        results = await _employeeRepository.FindAsync(q => q.Company("1").FieldCondition("myfield1", ComparisonOperator.Contains, "other"));
        Assert.Single(results.Documents);
        Assert.Equal(30, results.Documents.First().Age);
    }

    [Fact]
    public async Task FieldNotContains_WithCustomFieldName_ResolvesToIndexSlot()
    {
        // Arrange
        await _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition
        {
            EntityType = nameof(EmployeeWithCustomFields),
            TenantKey = "1",
            Name = "MyField1",
            IndexType = StringFieldType.IndexType
        });

        var single = EmployeeWithCustomFieldsGenerator.Generate(age: 19);
        single.CompanyId = "1";
        single.Data["MyField1"] = "hello";
        var multi = EmployeeWithCustomFieldsGenerator.Generate(age: 25);
        multi.CompanyId = "1";
        multi.Data["MyField1"] = "hello world";
        var other = EmployeeWithCustomFieldsGenerator.Generate(age: 30);
        other.CompanyId = "1";
        other.Data["MyField1"] = "other";
        await _employeeRepository.AddAsync([single, multi, other], o => o.ImmediateConsistency());

        // Act — NOT "hello" excludes both "hello" and "hello world", leaves "other"
        var results = await _employeeRepository.FindAsync(q => q.Company("1").FieldCondition("myfield1", ComparisonOperator.NotContains, "hello"));
        Assert.Single(results.Documents);
        Assert.Equal(30, results.Documents.First().Age);

        // Act — NOT "hello world" excludes only "hello world" (requires both tokens), leaves "hello" and "other"
        results = await _employeeRepository.FindAsync(q => q.Company("1").FieldCondition("myfield1", ComparisonOperator.NotContains, "hello world"));
        Assert.Equal(2, results.Documents.Count);
        Assert.DoesNotContain(results.Documents, d => d.Age == 25);

        // Act — NOT "world hello" reversed order — same as NOT "hello world"
        results = await _employeeRepository.FindAsync(q => q.Company("1").FieldCondition("myfield1", ComparisonOperator.NotContains, "world hello"));
        Assert.Equal(2, results.Documents.Count);
        Assert.DoesNotContain(results.Documents, d => d.Age == 25);

        // Act — NOT "other" excludes only "other", leaves both hello docs
        results = await _employeeRepository.FindAsync(q => q.Company("1").FieldCondition("myfield1", ComparisonOperator.NotContains, "other"));
        Assert.Equal(2, results.Documents.Count);
        Assert.DoesNotContain(results.Documents, d => d.Age == 30);
    }

    [Fact]
    public async Task CanHandleWrongFieldValueType()
    {
        var customField = await _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition
        {
            EntityType = nameof(EmployeeWithCustomFields),
            TenantKey = "1",
            Name = "MyField1",
            IndexType = IntegerFieldType.IndexType
        });
        Assert.Equal(1, customField.IndexSlot);

        var employee = EmployeeWithCustomFieldsGenerator.Generate(age: 19);
        employee.CompanyId = "1";
        employee.PhoneNumbers.Add(new PhoneInfo { Number = "214-222-2222" });
        employee.Data["MyField1"] = "hey";
        await _employeeRepository.AddAsync(employee, o => o.ImmediateConsistency());

        var results = await _employeeRepository.FindAsync(q => q.Company("1").FilterExpression("_exists_:myfield1"), o => o.QueryLogLevel(LogLevel.Information));
        var employees = results.Documents.ToArray();
        Assert.Empty(employees);
    }

    [Fact]
    public async Task CanUseCalculatedFieldType()
    {
        await _customFieldDefinitionRepository.AddAsync([
            new CustomFieldDefinition
            {
                EntityType = nameof(EmployeeWithCustomFields),
                TenantKey = "1",
                Name = "Field1",
                IndexType = IntegerFieldType.IndexType
            },
            new CustomFieldDefinition
            {
                EntityType = nameof(EmployeeWithCustomFields),
                TenantKey = "1",
                Name = "Field2",
                IndexType = IntegerFieldType.IndexType
            },
            new CustomFieldDefinition
            {
                EntityType = nameof(EmployeeWithCustomFields),
                TenantKey = "1",
                Name = "Calculated",
                IndexType = IntegerFieldType.IndexType,
                ProcessMode = CustomFieldProcessMode.AlwaysProcess,
                Data = new Dictionary<string, object?> { { "Expression", "(source.Data.Field1 || 0) + (source.Data.Field2 || 0)" } }
            }
        ]);

        var employee = EmployeeWithCustomFieldsGenerator.Generate(age: 19);
        employee.CompanyId = "1";
        employee.PhoneNumbers.Add(new PhoneInfo { Number = "214-222-2222" });
        employee.Data["Field1"] = 1;
        employee.Data["Field2"] = 2;
        await _employeeRepository.AddAsync(employee, o => o.ImmediateConsistency());

        var results = await _employeeRepository.FindAsync(q => q.Company("1").FilterExpression("calculated:3"), o => o.QueryLogLevel(LogLevel.Information));
        var employees = results.Documents.ToArray();
        Assert.Single(employees);
    }

    [Fact]
    public async Task CalculatedField_InputsRemoved_ClearsStaleDataValue()
    {
        // Arrange — raw sum (no || 0); missing operands become NaN in JS → null calculated value
        await _customFieldDefinitionRepository.AddAsync([
            new CustomFieldDefinition
            {
                EntityType = nameof(EmployeeWithCustomFields),
                TenantKey = "1",
                Name = "SumA",
                IndexType = IntegerFieldType.IndexType
            },
            new CustomFieldDefinition
            {
                EntityType = nameof(EmployeeWithCustomFields),
                TenantKey = "1",
                Name = "SumB",
                IndexType = IntegerFieldType.IndexType
            },
            new CustomFieldDefinition
            {
                EntityType = nameof(EmployeeWithCustomFields),
                TenantKey = "1",
                Name = "SumCalc",
                IndexType = IntegerFieldType.IndexType,
                ProcessMode = CustomFieldProcessMode.AlwaysProcess,
                Data = new Dictionary<string, object?> { { "Expression", "source.Data.SumA + source.Data.SumB" } }
            }
        ]);

        var employee = EmployeeWithCustomFieldsGenerator.Generate(age: 21);
        employee.CompanyId = "1";
        employee.Data["SumA"] = 10;
        employee.Data["SumB"] = 20;
        await _employeeRepository.AddAsync(employee, o => o.ImmediateConsistency());

        var indexed = await _employeeRepository.FindAsync(q => q.Company("1").FilterExpression("sumcalc:30"), o => o.QueryLogLevel(LogLevel.Information));
        Assert.Single(indexed.Documents);

        // Act — remove inputs; expression yields NaN → ProcessValueAsync returns null → Data must drop SumCalc
        var loaded = await _employeeRepository.GetByIdAsync(employee.Id, o => o.Cache(false));
        Assert.NotNull(loaded);
        loaded.Data.Remove("SumA");
        loaded.Data.Remove("SumB");
        await _employeeRepository.SaveAsync(loaded, o => o.ImmediateConsistency());

        // Assert
        var after = await _employeeRepository.GetByIdAsync(employee.Id, o => o.Cache(false));
        Assert.NotNull(after);
        Assert.False(after.Data.ContainsKey("SumCalc"));

        var staleSearch = await _employeeRepository.FindAsync(q => q.Company("1").FilterExpression("sumcalc:30"), o => o.QueryLogLevel(LogLevel.Information));
        Assert.Empty(staleSearch.Documents);
    }

    [Fact]
    public async Task CalculatedField_PartialInputs_ComputesFromAvailableValues()
    {
        // Arrange
        await _customFieldDefinitionRepository.AddAsync([
            new CustomFieldDefinition
            {
                EntityType = nameof(EmployeeWithCustomFields),
                TenantKey = "1",
                Name = "Part1",
                IndexType = IntegerFieldType.IndexType
            },
            new CustomFieldDefinition
            {
                EntityType = nameof(EmployeeWithCustomFields),
                TenantKey = "1",
                Name = "Part2",
                IndexType = IntegerFieldType.IndexType
            },
            new CustomFieldDefinition
            {
                EntityType = nameof(EmployeeWithCustomFields),
                TenantKey = "1",
                Name = "PartSum",
                IndexType = IntegerFieldType.IndexType,
                ProcessMode = CustomFieldProcessMode.AlwaysProcess,
                Data = new Dictionary<string, object?> { { "Expression", "(source.Data.Part1 || 0) + (source.Data.Part2 || 0)" } }
            }
        ]);

        var employee = EmployeeWithCustomFieldsGenerator.Generate(age: 22);
        employee.CompanyId = "1";
        employee.Data["Part1"] = 5;
        await _employeeRepository.AddAsync(employee, o => o.ImmediateConsistency());

        // Assert — only Part1 present → sum 5
        var r5 = await _employeeRepository.FindAsync(q => q.Company("1").FilterExpression("partsum:5"), o => o.QueryLogLevel(LogLevel.Information));
        Assert.Single(r5.Documents);

        // Act — add Part2
        var loaded = await _employeeRepository.GetByIdAsync(employee.Id, o => o.Cache(false));
        Assert.NotNull(loaded);
        loaded.Data["Part2"] = 3;
        await _employeeRepository.SaveAsync(loaded, o => o.ImmediateConsistency());

        // Assert — 5 + 3 = 8
        var r8 = await _employeeRepository.FindAsync(q => q.Company("1").FilterExpression("partsum:8"), o => o.QueryLogLevel(LogLevel.Information));
        Assert.Single(r8.Documents);
    }

    [Fact]
    public async Task CalculatedField_UpdateTransitionsToNull_RemovesFromDataAndIdx()
    {
        // Arrange
        await _customFieldDefinitionRepository.AddAsync([
            new CustomFieldDefinition
            {
                EntityType = nameof(EmployeeWithCustomFields),
                TenantKey = "1",
                Name = "Upd1",
                IndexType = IntegerFieldType.IndexType
            },
            new CustomFieldDefinition
            {
                EntityType = nameof(EmployeeWithCustomFields),
                TenantKey = "1",
                Name = "Upd2",
                IndexType = IntegerFieldType.IndexType
            },
            new CustomFieldDefinition
            {
                EntityType = nameof(EmployeeWithCustomFields),
                TenantKey = "1",
                Name = "UpdCalc",
                IndexType = IntegerFieldType.IndexType,
                ProcessMode = CustomFieldProcessMode.AlwaysProcess,
                Data = new Dictionary<string, object?> { { "Expression", "source.Data.Upd1 + source.Data.Upd2" } }
            }
        ]);

        var employee = EmployeeWithCustomFieldsGenerator.Generate(age: 23);
        employee.CompanyId = "1";
        employee.Data["Upd1"] = 1;
        employee.Data["Upd2"] = 2;
        await _employeeRepository.AddAsync(employee, o => o.ImmediateConsistency());

        var withValue = await _employeeRepository.GetByIdAsync(employee.Id, o => o.Cache(false));
        Assert.NotNull(withValue);
        Assert.True(withValue.Data.TryGetValue("UpdCalc", out var updCalcValue));
        Assert.NotNull(updCalcValue);
        Assert.Equal(3, Convert.ToInt32(updCalcValue));
        var indexedBefore = await _employeeRepository.FindAsync(q => q.Company("1").FilterExpression("updcalc:3"), o => o.QueryLogLevel(LogLevel.Information));
        Assert.Single(indexedBefore.Documents);

        // Act — remove inputs → NaN → null calculated
        withValue.Data.Remove("Upd1");
        withValue.Data.Remove("Upd2");
        await _employeeRepository.SaveAsync(withValue, o => o.ImmediateConsistency());

        // Assert
        var cleared = await _employeeRepository.GetByIdAsync(employee.Id, o => o.Cache(false));
        Assert.NotNull(cleared);
        Assert.False(cleared.Data.ContainsKey("UpdCalc"));

        // Assert — indexed field (idx.*) no longer matches previous value (verifies Idx side in ES, not only Data)
        var noHit = await _employeeRepository.FindAsync(q => q.Company("1").FilterExpression("updcalc:3"), o => o.QueryLogLevel(LogLevel.Information));
        Assert.Empty(noHit.Documents);
    }

    [Fact]
    public void CustomFieldHelpers_OnNonCustomFieldType_ThrowsRepositoryException()
    {
        var repo = new CustomFieldHelperTestRepository(_configuration.Employees);
        var employee = EmployeeGenerator.Generate();

        Assert.Throws<RepositoryException>(() => { repo.TestGetDocumentCustomFields(employee); });
        Assert.Throws<RepositoryException>(() => { repo.TestGetDocumentCustomField(employee, "field"); });
        Assert.Throws<RepositoryException>(() => repo.TestSetDocumentCustomField(employee, "field", "value"));
        Assert.Throws<RepositoryException>(() => repo.TestRemoveDocumentCustomField(employee, "field"));
        Assert.Throws<RepositoryException>(() => { repo.TestGetDocumentIdx(employee); });
    }
}

internal sealed class CustomFieldHelperTestRepository : ElasticRepositoryBase<Employee>
{
    public CustomFieldHelperTestRepository(IIndex index) : base(index) { }

    public IDictionary<string, object?> TestGetDocumentCustomFields(Employee doc) => GetDocumentCustomFields(doc);
    public object? TestGetDocumentCustomField(Employee doc, string name) => GetDocumentCustomField(doc, name);
    public void TestSetDocumentCustomField(Employee doc, string name, object? value) => SetDocumentCustomField(doc, name, value);
    public void TestRemoveDocumentCustomField(Employee doc, string name) => RemoveDocumentCustomField(doc, name);
    public IDictionary<string, object> TestGetDocumentIdx(Employee doc) => GetDocumentIdx(doc);
}
