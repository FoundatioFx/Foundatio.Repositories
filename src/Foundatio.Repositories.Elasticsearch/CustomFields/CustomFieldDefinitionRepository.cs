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
using Foundatio.Repositories.Options;
using Microsoft.Extensions.Logging;
using Nest;

namespace Foundatio.Repositories.Elasticsearch.CustomFields;

public interface ICustomFieldDefinitionRepository : ISearchableRepository<CustomFieldDefinition>
{
    Task<IDictionary<string, CustomFieldDefinition>> GetFieldMappingAsync(string entityType, string tenantKey);
    Task<FindResults<CustomFieldDefinition>> FindByTenantAsync(string entityType, string tenantKey);
    Task<CustomFieldDefinition> AddFieldAsync(string entityType, string tenantKey, string name, string indexType, string description = null, int displayOrder = 0, IDictionary<string, object> data = null);
}

public class CustomFieldDefinitionRepository : ElasticRepositoryBase<CustomFieldDefinition>, ICustomFieldDefinitionRepository
{
    private readonly ILockProvider _lockProvider;
    private readonly ICacheClient _cache;

    public CustomFieldDefinitionRepository(CustomFieldDefinitionIndex index, ILockProvider lockProvider) : base(index)
    {
        _lockProvider = lockProvider;

        // don't want individual docs to be cached
        DisableCache();
        _cache = index.Configuration.Cache;

        OriginalsEnabled = true;
        DefaultConsistency = Consistency.Immediate;

        AddPropertyRequiredForRemove(d => d.EntityType, d => d.TenantKey, d => d.IndexType, d => d.IndexSlot);

        DocumentsChanged.AddHandler(OnDocumentsChanged);
    }

    public async Task<IDictionary<string, CustomFieldDefinition>> GetFieldMappingAsync(string entityType, string tenantKey)
    {
        string cacheKey = GetMappingCacheKey(entityType, tenantKey);
        var cachedMapping = await _cache.GetAsync<Dictionary<string, CustomFieldDefinition>>(cacheKey).AnyContext();
        if (cachedMapping.HasValue)
            return cachedMapping.Value;

        var fieldMapping = new Dictionary<string, CustomFieldDefinition>(StringComparer.OrdinalIgnoreCase);

        var fields = await FindAsync(q => q
            .FieldEquals(cf => cf.EntityType, entityType)
            .FieldEquals(cf => cf.TenantKey, tenantKey),
            o => o.PageLimit(1000)).AnyContext();

        do
        {
            foreach (var customField in fields.Documents)
                fieldMapping[customField.Name] = customField;
        } while (await fields.NextPageAsync().AnyContext());

        fieldMapping = fieldMapping.OrderBy(f => f.Value.ProcessOrder).ToDictionary(kvp => kvp.Key, kvp => kvp.Value, StringComparer.OrdinalIgnoreCase);

        if (fieldMapping.Count > 0)
            await _cache.AddAsync(cacheKey, fieldMapping, TimeSpan.FromMinutes(15)).AnyContext();

        return fieldMapping;
    }

    public Task<FindResults<CustomFieldDefinition>> FindByTenantAsync(string entityType, string tenantKey)
    {
        return FindAsync(q => q.FieldEquals(cf => cf.EntityType, entityType).FieldEquals(cf => cf.TenantKey, tenantKey), o => o.PageLimit(1000));
    }

    public Task<long> RemoveByTenantAsync(string entityType, string tenantKey)
    {
        return RemoveAllAsync(q => q.FieldEquals(cf => cf.EntityType, entityType).FieldEquals(cf => cf.TenantKey, tenantKey));
    }

    public Task<CustomFieldDefinition> AddFieldAsync(string entityType, string tenantKey, string name, string indexType, string description = null, int displayOrder = 0, IDictionary<string, object> data = null)
    {
        var customField = new CustomFieldDefinition
        {
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

    public override async Task AddAsync(IEnumerable<CustomFieldDefinition> documents, ICommandOptions options = null)
    {
        var documentArray = documents as CustomFieldDefinition[] ?? documents.ToArray();
        var fieldScopes = documentArray.GroupBy(d => (d.EntityType, d.TenantKey, d.IndexType)).ToArray();
        string[] lockKeys = fieldScopes.Select(f => GetLockName(f.Key.EntityType, f.Key.TenantKey, f.Key.IndexType)).ToArray();
        await using var lck = await _lockProvider.AcquireAsync(lockKeys, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1)).AnyContext();
        if (lck is null)
            throw new Exception($"Failed to acquire lock: {String.Join(", ", lockKeys)}");

        foreach (var fieldScope in fieldScopes)
        {
            string slotFieldScopeKey = GetSlotFieldScopeCacheKey(fieldScope.Key.EntityType, fieldScope.Key.TenantKey, fieldScope.Key.IndexType);
            string namesFieldScopeKey = GetNamesFieldScopeCacheKey(fieldScope.Key.EntityType, fieldScope.Key.TenantKey);

            var usedNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var availableSlots = new Queue<int>();
            var availableSlotsCache = await _cache.GetListAsync<int>(slotFieldScopeKey).AnyContext();
            var usedNamesCache = await _cache.GetListAsync<string>(namesFieldScopeKey).AnyContext();

            if (availableSlotsCache.HasValue && usedNamesCache.HasValue && availableSlotsCache.Value.Count > 0)
            {
                foreach (int availableSlot in availableSlotsCache.Value.OrderBy(s => s))
                    availableSlots.Enqueue(availableSlot);

                foreach (string usedName in usedNamesCache.Value.ToArray())
                    usedNames.Add(usedName);

                _logger.LogTrace("Got cached list of {SlotCount} available slots for {FieldScope}", availableSlots.Count, slotFieldScopeKey);
            }
            else
            {
                var usedSlots = new List<int>();
                var existingFields = await FindAsync(q => q
                    .FieldEquals(cf => cf.EntityType, fieldScope.Key.EntityType)
                    .FieldEquals(cf => cf.TenantKey, fieldScope.Key.TenantKey)
                    .Include(cf => cf.IndexType)
                    .Include(cf => cf.IndexSlot)
                    .Include(cf => cf.Name)
                    .Include(cf => cf.IsDeleted),
                    o => o.IncludeSoftDeletes().PageLimit(1000).QueryLogLevel(Microsoft.Extensions.Logging.LogLevel.Information)).AnyContext();

                do
                {
                    usedSlots.AddRange(existingFields.Documents.Where(f => f.IndexType == fieldScope.Key.IndexType).Select(d => d.IndexSlot));
                    usedNames.AddRange(existingFields.Documents.Where(d => !d.IsDeleted).Select(d => d.Name));
                } while (await existingFields.NextPageAsync().AnyContext());

                int slotBatchSize = fieldScope.Count() + 25;
                int slot = 1;
                while (availableSlots.Count < slotBatchSize)
                {
                    if (!usedSlots.Contains(slot))
                        availableSlots.Enqueue(slot);

                    slot++;
                }

                _logger.LogTrace("Found {FieldCount} fields with {SlotCount} used slots for {FieldScope}", existingFields.Total, usedSlots.Count, slotFieldScopeKey);
                await _cache.ListAddAsync(slotFieldScopeKey, availableSlots.ToArray(), TimeSpan.FromMinutes(5)).AnyContext();
                await _cache.ListAddAsync(namesFieldScopeKey, usedNames.ToArray(), TimeSpan.FromMinutes(5)).AnyContext();
            }

            foreach (var doc in fieldScope)
            {
                if (doc.IndexSlot != 0)
                    throw new DocumentValidationException("IndexSlot can't be assigned.");

                if (usedNames.Contains(doc.Name))
                    throw new DocumentValidationException($"Custom field with name {doc.Name} already exists");

                int availableSlot = availableSlots.Dequeue();
                doc.IndexSlot = availableSlot;

                await _cache.ListRemoveAsync(slotFieldScopeKey, [availableSlot]).AnyContext();
                await _cache.ListAddAsync(namesFieldScopeKey, [doc.Name]).AnyContext();
                _logger.LogTrace("New field {FieldName} using slot {IndexSlot} for {FieldScope}", doc.Name, doc.IndexSlot, slotFieldScopeKey);
            }
        }

        await base.AddAsync(documentArray, options).AnyContext();
    }

    protected override Task ValidateAndThrowAsync(CustomFieldDefinition document)
    {
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

    private async Task OnDocumentsChanged(object source, DocumentsChangeEventArgs<CustomFieldDefinition> args)
    {
        if (args.ChangeType == ChangeType.Saved)
        {
            foreach (var doc in args.Documents)
            {
                if (doc.Original.EntityType != doc.Value.EntityType)
                    throw new DocumentValidationException("EntityType can't be changed.");
                if (doc.Original.TenantKey != doc.Value.TenantKey)
                    throw new DocumentValidationException("TenantKey can't be changed.");
                if (doc.Original.IndexSlot != doc.Value.IndexSlot)
                    throw new DocumentValidationException("IndexSlot can't be changed.");

                if (doc.Value.IsDeleted)
                {
                    string namesFieldScopeKey = GetNamesFieldScopeCacheKey(doc.Value.EntityType, doc.Value.TenantKey);
                    await _cache.ListRemoveAsync(namesFieldScopeKey, [doc.Value.Name]).AnyContext();
                }
            }
        }
        else if (args.ChangeType == ChangeType.Removed)
        {
            foreach (var doc in args.Documents)
            {
                string slotFieldScopeKey = GetSlotFieldScopeCacheKey(doc.Value.EntityType, doc.Value.TenantKey, doc.Value.IndexType);
                string namesFieldScopeKey = GetNamesFieldScopeCacheKey(doc.Value.EntityType, doc.Value.TenantKey);
                await _cache.ListAddAsync(slotFieldScopeKey, [doc.Value.IndexSlot]).AnyContext();
                await _cache.ListRemoveAsync(namesFieldScopeKey, [doc.Value.Name]).AnyContext();
            }
        }
    }

    private string GetLockName(string entityType, string tenantKey, string indexType)
    {
        return $"customfield:{entityType}:{tenantKey}:{indexType}";
    }

    private string GetMappingCacheKey(string entityType, string tenantKey)
    {
        return $"customfield:{entityType}:{tenantKey}";
    }

    private string GetSlotFieldScopeCacheKey(string entityType, string tenantKey, string indexType)
    {
        return $"customfield:{entityType}:{tenantKey}:{indexType}:slots";
    }

    private string GetNamesFieldScopeCacheKey(string entityType, string tenantKey)
    {
        return $"customfield:{entityType}:{tenantKey}:names";
    }

    protected override async Task InvalidateCacheByQueryAsync(IRepositoryQuery<CustomFieldDefinition> query)
    {
        await base.InvalidateCacheByQueryAsync(query).AnyContext();

        var conditions = query.GetFieldConditions();
        var entityTypeCondition = conditions.FirstOrDefault(c => c.Field == InferField(d => d.EntityType) && c.Operator == ComparisonOperator.Equals);
        if (entityTypeCondition == null || String.IsNullOrEmpty(entityTypeCondition.Value?.ToString()))
            return;

        await _cache.RemoveAsync(GetMappingCacheKey(entityTypeCondition.Value.ToString(), GetTenantKey(query))).AnyContext();
    }

    protected override async Task InvalidateCacheAsync(IReadOnlyCollection<ModifiedDocument<CustomFieldDefinition>> documents, ChangeType? changeType = null)
    {
        await base.InvalidateCacheAsync(documents, changeType).AnyContext();

        if (documents.Count == 0)
        {
            await _cache.RemoveByPrefixAsync("customfield").AnyContext();
            _logger.LogInformation("Cleared all custom field mappings from cache due to change {ChangeType}", changeType);
        }

        var cacheKeys = documents.Select(d => GetMappingCacheKey(d.Value.EntityType, d.Value.TenantKey)).Distinct().ToList();
        await _cache.RemoveAllAsync(cacheKeys).AnyContext();
    }
}

public class CustomFieldDefinitionIndex : VersionedIndex<CustomFieldDefinition>
{
    private readonly int _replicas;

    public CustomFieldDefinitionIndex(IElasticConfiguration configuration, string name = "customfield", int replicas = 1) : base(configuration, name, 1)
    {
        _replicas = replicas;
    }

    public override TypeMappingDescriptor<CustomFieldDefinition> ConfigureIndexMapping(TypeMappingDescriptor<CustomFieldDefinition> map)
    {
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

    public override CreateIndexDescriptor ConfigureIndex(CreateIndexDescriptor idx)
    {
        return base.ConfigureIndex(idx).Settings(s => s
            .NumberOfShards(1)
            .NumberOfReplicas(_replicas));
    }
}
