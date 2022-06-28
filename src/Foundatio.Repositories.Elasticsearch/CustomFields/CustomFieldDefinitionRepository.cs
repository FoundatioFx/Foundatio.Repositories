using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Lock;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Models;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.CustomFields;

public class CustomFieldDefinitionRepository : ElasticRepositoryBase<CustomFieldDefinition> {
    private readonly ILockProvider _lockProvider;

    public CustomFieldDefinitionRepository(CustomFieldDefinitionIndex index, ILockProvider lockProvider) : base(index) {
        _lockProvider = lockProvider;

        DisableCache();
        OriginalsEnabled = true;
        DefaultConsistency = Consistency.Immediate;

        DocumentsChanging.AddHandler(OnDocumentsChanging);
    }

    protected override Task ValidateAndThrowAsync(CustomFieldDefinition document) {
        if (String.IsNullOrEmpty(document.EntityType))
            throw new ArgumentException();

        if (String.IsNullOrEmpty(document.TenantKey))
            throw new ArgumentException();

        if (String.IsNullOrEmpty(document.IndexType))
            throw new ArgumentException();

        if (String.IsNullOrEmpty(document.Name))
            throw new ArgumentException();

        return Task.CompletedTask;
    }

    public override async Task AddAsync(IEnumerable<CustomFieldDefinition> documents, ICommandOptions options = null) {
        var lockNames = documents.GroupBy(d => (d.EntityType, d.TenantKey)).Select(g => $"customfield:{g.Key.EntityType}:{g.Key.TenantKey}");
        await using var _ = await _lockProvider.AcquireAsync(lockNames, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
        await base.AddAsync(documents, options);
    }

    private async Task OnDocumentsChanging(object source, DocumentsChangeEventArgs<CustomFieldDefinition> args) {
        if (args.ChangeType == ChangeType.Added) {
            var fieldScopes = args.Documents.GroupBy(d => (d.Value.EntityType,  d.Value.TenantKey));

            foreach (var fieldScope in fieldScopes) {
                var fieldList = new List<(string EntityType, string TenantKey, string IndexType, int IndexSlot)>();
                var existingFields = await FindAsync(q => q
                    .FieldEquals(cf => cf.EntityType, fieldScope.Key.EntityType)
                    .FieldEquals(cf => cf.TenantKey, fieldScope.Key.TenantKey)
                    .Include(cf => cf.IndexType).Include(cf => cf.IndexSlot),
                    o => o.IncludeSoftDeletes().ImmediateConsistency().PageLimit(1000));

                do {
                    fieldList.AddRange(existingFields.Documents.Select(d => (d.EntityType, d.TenantKey, d.IndexType, d.IndexSlot)));
                } while (await existingFields.NextPageAsync());

                var indexTypeFields = fieldList.GroupBy(f => f.IndexType).ToDictionary(d => d.Key, d => ( Used: new HashSet<int>(d.Select(d => d.IndexSlot)), CurrentSlot: 1 ));

                foreach (var doc in fieldScope) {
                    if (!indexTypeFields.TryGetValue(doc.Value.IndexType, out var indexTypeSlots)) {
                        indexTypeSlots = (new HashSet<int>(), 1);
                        indexTypeFields.Add(doc.Value.IndexType, indexTypeSlots);
                    }

                    while (indexTypeSlots.Used.Contains(indexTypeSlots.CurrentSlot))
                        indexTypeSlots.CurrentSlot++;

                    doc.Value.IndexSlot = indexTypeSlots.CurrentSlot;

                    indexTypeSlots.CurrentSlot++;
                }
            }
        } else if (args.ChangeType == ChangeType.Saved) {
            foreach (var doc in args.Documents) {
                if (doc.Original.IndexType != doc.Value.IndexType || doc.Original.IndexSlot != doc.Value.IndexSlot)
                    throw new DocumentException("IndexType and IndexSlot can't be changed.");
            }
        }
    }
}

public class CustomFieldDefinitionIndex : VersionedIndex<CustomFieldDefinition> {
    private readonly int _replicas;

    public CustomFieldDefinitionIndex(IElasticConfiguration configuration, string name = "customfield", int replicas = 1) : base(configuration, name, 1) {
        _replicas = replicas;
    }

    public override TypeMappingDescriptor<CustomFieldDefinition> ConfigureIndexMapping(TypeMappingDescriptor<CustomFieldDefinition> map) {
        return map
            .Dynamic(false)
            .Properties(p => p
                .SetupDefaults()
                .Keyword(f => f.Name(e => e.Id))
                .Keyword(f => f.Name(e => e.EntityType))
                .Keyword(f => f.Name(e => e.TenantKey))
                .Keyword(f => f.Name(e => e.IndexType))
                .Number(f => f.Name(e => e.IndexSlot))
                .Date(f => f.Name(e => e.CreatedUtc))
                .Date(f => f.Name(e => e.UpdatedUtc))
            );
    }

    public override CreateIndexDescriptor ConfigureIndex(CreateIndexDescriptor idx) {
        return base.ConfigureIndex(idx).Settings(s => s
            .NumberOfShards(1)
            .NumberOfReplicas(_replicas));
    }
}
