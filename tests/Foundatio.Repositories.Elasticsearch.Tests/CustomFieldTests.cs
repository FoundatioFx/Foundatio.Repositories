using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Lock;
using Foundatio.Repositories.Elasticsearch.CustomFields;
using Microsoft.Extensions.Logging;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.Repositories.Elasticsearch.Tests;

public sealed class CustomFieldTests : ElasticRepositoryTestBase {
    private readonly CustomFieldDefinitionRepository _customFieldDefinitionRepository;
    private readonly ILockProvider _lockProvider;

    public CustomFieldTests(ITestOutputHelper output) : base(output) {
        _lockProvider = new CacheLockProvider(_cache, _messageBus, Log);
        _customFieldDefinitionRepository = new CustomFieldDefinitionRepository(_configuration.CustomFieldDefinition, _lockProvider);
    }

    public override async Task InitializeAsync() {
        await base.InitializeAsync();
        await RemoveDataAsync();
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

        customField = await _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition {
            EntityType = "Employee",
            TenantKey = "1",
            Name = "MyField2",
            IndexType = "string"
        });
        Assert.Equal(2, customField.IndexSlot);

        customField = await _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition {
            EntityType = "Employee",
            TenantKey = "1",
            Name = "MyField3",
            IndexType = "string"
        });
        Assert.Equal(3, customField.IndexSlot);
    }

    [Fact]
    public async Task CanAddNewFieldsAndReserveSlotsConcurrently() {
        Log.SetLogLevel<CustomFieldDefinitionRepository>(LogLevel.Trace);

        const int COUNT = 100;
        await Parallel.ForEachAsync(Enumerable.Range(1, COUNT), async (index, ct) => {
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
}
