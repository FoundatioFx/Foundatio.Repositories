using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Caching;
using Foundatio.Lock;
using Foundatio.Messaging;
using Foundatio.Repositories.Elasticsearch.CustomFields;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories;
using Foundatio.Repositories.Elasticsearch.Tests.Repositories.Models;
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
            Name = "Blah",
            IndexType = "string"
        });
        Assert.Equal(1, customField.IndexSlot);

        customField = await _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition {
            EntityType = "Employee",
            TenantKey = "1",
            Name = "Blah",
            IndexType = "string"
        });
        Assert.Equal(2, customField.IndexSlot);

        customField = await _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition {
            EntityType = "Employee",
            TenantKey = "1",
            Name = "Blah",
            IndexType = "string"
        });
        Assert.Equal(3, customField.IndexSlot);
    }

    [Fact]
    public async Task CanAddNewFieldsAndReserveSlotsConcurrently() {
        await Parallel.ForEachAsync(Enumerable.Range(1, 1000), async (index, ct) => {
            var customField = await _customFieldDefinitionRepository.AddAsync(new CustomFieldDefinition {
                EntityType = "Employee",
                TenantKey = "1",
                Name = "Blah",
                IndexType = "string"
            });

            Assert.NotNull(customField);
            Assert.InRange(customField.IndexSlot, 1, 1000);
        });

        var usedSlots = new HashSet<int>();
        var customFields = await _customFieldDefinitionRepository.GetAllAsync(o => o.PageLimit(1000));
        foreach (var doc in customFields.Documents) {
            Assert.DoesNotContain(doc.IndexSlot, usedSlots);
            usedSlots.Add(doc.IndexSlot);
        }

        Assert.Equal(1000, usedSlots.Count);
    }
}
