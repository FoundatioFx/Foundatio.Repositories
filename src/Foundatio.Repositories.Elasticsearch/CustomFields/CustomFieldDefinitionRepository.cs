using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Caching;
using Foundatio.Lock;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Models;
using Microsoft.Extensions.Logging;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.CustomFields;

public interface ICustomFieldDefinitionRepository : ISearchableRepository<CustomFieldDefinition> {
    Task<IDictionary<string, FieldIndexInfo>> GetFieldMappingAsync(string entityType, string tenantKey);
    Task<FindResults<CustomFieldDefinition>> FindByTenantAsync(string entityType, string tenantKey);
    Task<CustomFieldDefinition> AddFieldAsync(string entityType, string tenantKey, string name, string indexType, string description = null, int displayOrder = 0, IDictionary<string, object> data = null);
}

public class FieldIndexInfo {
    public string IndexType { get; set; }
    public int IndexSlot { get; set; }

    private string _idxName = null;
    public string GetIdxName() {
        if (_idxName == null)
            _idxName = $"{IndexType}-{IndexSlot}";

        return _idxName;
    }

    public override string ToString() {
        return GetIdxName();
    }
}

public class CustomFieldDefinitionRepository : ElasticRepositoryBase<CustomFieldDefinition>, ICustomFieldDefinitionRepository {
    private readonly ILockProvider _lockProvider;
    private readonly ICacheClient _cache;

    public CustomFieldDefinitionRepository(CustomFieldDefinitionIndex index, ILockProvider lockProvider) : base(index) {
        _lockProvider = lockProvider;
        _cache = index.Configuration.Cache;

        DisableCache();
        OriginalsEnabled = true;
        DefaultConsistency = Consistency.Immediate;

        DocumentsChanging.AddHandler(OnDocumentsChanging);
    }

    public async Task<IDictionary<string, FieldIndexInfo>> GetFieldMappingAsync(string entityType, string tenantKey) {
        string cacheKey = $"mapping:{entityType}:{tenantKey}";
        var cachedMapping = await Cache.GetAsync<Dictionary<string, FieldIndexInfo>>(cacheKey);
        if (cachedMapping.HasValue)
            return cachedMapping.Value;

        var fieldMapping = new Dictionary<string, FieldIndexInfo>(StringComparer.OrdinalIgnoreCase);

        var fields = await FindAsync(q => q
            .FieldEquals(cf => cf.EntityType, entityType)
            .FieldEquals(cf => cf.TenantKey, tenantKey)
            .Include(cf => cf.Name)
            .Include(cf => cf.IndexType)
            .Include(cf => cf.IndexSlot),
            o => o.PageLimit(1000));

        do {
            foreach (var customField in fields.Documents)
                fieldMapping[customField.Name] = new FieldIndexInfo {
                    IndexType = customField.IndexType,
                    IndexSlot = customField.IndexSlot
                };
        } while (await fields.NextPageAsync());

        await Cache.AddAsync(cacheKey, fieldMapping, TimeSpan.FromMinutes(15));

        return fieldMapping;
    }

    public Task<FindResults<CustomFieldDefinition>> FindByTenantAsync(string entityType, string tenantKey) {
        return FindAsync(q => q.FieldEquals(cf => cf.EntityType, entityType).FieldEquals(cf => cf.TenantKey, tenantKey), o => o.PageLimit(1000));
    }

    public Task<CustomFieldDefinition> AddFieldAsync(string entityType, string tenantKey, string name, string indexType, string description = null, int displayOrder = 0, IDictionary<string, object> data = null) {
        var customField = new CustomFieldDefinition {
            EntityType = entityType,
            TenantKey = tenantKey,
            Name = name,
            IndexType = indexType,
            Description = description,
            DisplayOrder = displayOrder
        };

        if (data != null)
            customField.Data = data;

        return AddAsync(customField);
    }

    public override async Task AddAsync(IEnumerable<CustomFieldDefinition> documents, ICommandOptions options = null) {
        var fieldScopes = documents.GroupBy(d => (d.EntityType, d.TenantKey, d.IndexType));
        var lockKeys = fieldScopes.Select(f => $"customfield:{f.Key.EntityType}:{f.Key.TenantKey}:{f.Key.IndexType}");
        await using var _ = await _lockProvider.AcquireAsync(lockKeys, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));

        foreach (var fieldScope in fieldScopes) {
            string slotFieldScopeKey = $"customfield:{fieldScope.Key.EntityType}:{fieldScope.Key.TenantKey}:{fieldScope.Key.IndexType}:slots";
            string namesFieldScopeKey = $"customfield:{fieldScope.Key.EntityType}:{fieldScope.Key.TenantKey}:{fieldScope.Key.IndexType}:names";

            var usedNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var availableSlots = new Queue<int>();
            var availableSlotsCache = await _cache.GetListAsync<int>(slotFieldScopeKey);
            var usedNamesCache = await _cache.GetListAsync<string>(namesFieldScopeKey);

            if (availableSlotsCache.HasValue && usedNamesCache.HasValue && availableSlotsCache.Value.Count > 0) {
                foreach (var availableSlot in availableSlotsCache.Value.OrderBy(s => s))
                    availableSlots.Enqueue(availableSlot);

                foreach (var usedName in usedNamesCache.Value.ToArray())
                    usedNames.Add(usedName);

                _logger.LogTrace("Got cached list of {SlotCount} available slots for {FieldScope}", availableSlots.Count, slotFieldScopeKey);
            } else {
                var usedSlots = new List<int>();
                var existingFields = await FindAsync(q => q
                    .FieldEquals(cf => cf.EntityType, fieldScope.Key.EntityType)
                    .FieldEquals(cf => cf.TenantKey, fieldScope.Key.TenantKey)
                    .FieldEquals(cf => cf.IndexType, fieldScope.Key.IndexType)
                    .Include(cf => cf.IndexSlot)
                    .Include(cf => cf.Name),
                    o => o.IncludeSoftDeletes().PageLimit(1000));

                do {
                    usedSlots.AddRange(existingFields.Documents.Select(d => d.IndexSlot));
                    usedNames.AddRange(existingFields.Documents.Where(d => !d.IsDeleted).Select(d => d.Name));
                } while (await existingFields.NextPageAsync());

                int slotBatchSize = fieldScope.Count() + 25;
                int slot = 1;
                while (availableSlots.Count < slotBatchSize) {
                    if (!usedSlots.Contains(slot))
                        availableSlots.Enqueue(slot);

                    slot++;
                }

                _logger.LogTrace("Found {FieldCount} fields with {SlotCount} used slots for {FieldScope}", existingFields.Total, usedSlots.Count, slotFieldScopeKey);
                await _cache.ListAddAsync(slotFieldScopeKey, availableSlots.ToArray());
                await _cache.ListAddAsync(namesFieldScopeKey, usedNames.ToArray());
            }

            foreach (var doc in fieldScope) {
                if (doc.IndexSlot != 0)
                    throw new DocumentValidationException("IndexSlot can't be assigned.");

                if (usedNames.Contains(doc.Name))
                    throw new DocumentValidationException($"Custom field with name {doc.Name} already exists");

                int availableSlot = availableSlots.Dequeue();
                doc.IndexSlot = availableSlot;

                await _cache.ListRemoveAsync(slotFieldScopeKey, new[] { availableSlot });
                await _cache.ListAddAsync(namesFieldScopeKey, new[] { doc.Name });
                _logger.LogTrace("New field {FieldName} using slot {IndexSlot} for {FieldScope}", doc.Name, doc.IndexSlot, slotFieldScopeKey);
            }
        }

        await base.AddAsync(documents, options);
    }

    protected override Task ValidateAndThrowAsync(CustomFieldDefinition document) {
        if (String.IsNullOrEmpty(document.EntityType))
            throw new DocumentValidationException("EntityType is required");

        if (String.IsNullOrEmpty(document.TenantKey))
            throw new DocumentValidationException("TenantKey is required");

        if (String.IsNullOrEmpty(document.IndexType))
            throw new DocumentValidationException("IndexType is required");

        if (String.IsNullOrEmpty(document.Name))
            throw new DocumentValidationException("Name is required");

        return Task.CompletedTask;
    }

    private async Task OnDocumentsChanging(object source, DocumentsChangeEventArgs<CustomFieldDefinition> args) {
        if (args.ChangeType == ChangeType.Saved) {
            foreach (var doc in args.Documents) {
                if (doc.Original.EntityType != doc.Value.EntityType)
                    throw new DocumentValidationException("EntityType can't be changed.");
                if (doc.Original.TenantKey != doc.Value.TenantKey)
                    throw new DocumentValidationException("TenantKey can't be changed.");
                if (doc.Original.IndexSlot != doc.Value.IndexSlot)
                    throw new DocumentValidationException("IndexSlot can't be changed.");

                // eventually, changing these should be allowed and trigger a reindex
                if (doc.Original.IndexType != doc.Value.IndexType)
                    throw new DocumentValidationException("IndexType can't be changed.");
                if (doc.Original.Name != doc.Value.Name)
                    throw new DocumentValidationException("IndexSlot can't be changed.");

                if (doc.Value.IsDeleted) {
                    string namesFieldScopeKey = $"customfield:{doc.Value.EntityType}:{doc.Value.TenantKey}:{doc.Value.IndexType}:names";
                    await _cache.ListRemoveAsync(namesFieldScopeKey, new[] { doc.Value.Name });
                }
            }
        } else if (args.ChangeType == ChangeType.Removed) {
            foreach (var doc in args.Documents) {
                string slotFieldScopeKey = $"customfield:{doc.Value.EntityType}:{doc.Value.TenantKey}:{doc.Value.IndexType}:slots";
                string namesFieldScopeKey = $"customfield:{doc.Value.EntityType}:{doc.Value.TenantKey}:{doc.Value.IndexType}:names";
                await _cache.ListAddAsync(slotFieldScopeKey, new[] { doc.Value.IndexSlot });
                await _cache.ListRemoveAsync(namesFieldScopeKey, new[] { doc.Value.Name });
            }
        }
    }

    protected override async Task InvalidateCacheAsync(IReadOnlyCollection<ModifiedDocument<CustomFieldDefinition>> documents, ChangeType? changeType = null) {
        await base.InvalidateCacheAsync(documents, changeType);
        var cacheKeys = documents.GroupBy(d => (d.Value.EntityType, d.Value.TenantKey)).Select(g => $"mapping:{g.Key.EntityType}:{g.Key.TenantKey}");
        await Cache.RemoveAllAsync(cacheKeys);
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
