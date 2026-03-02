using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Clients.Elasticsearch.Mapping;
using Foundatio.Parsers.ElasticQueries;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Jobs;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Models;
using Microsoft.Extensions.Logging;

namespace Foundatio.Repositories.Elasticsearch.Configuration;

public interface IVersionedIndex : IIndex
{
    int Version { get; }
    string VersionedName { get; }
    Task<int> GetCurrentVersionAsync();
    ReindexWorkItem CreateReindexWorkItem(int currentVersion);
}

public class VersionedIndex : Index, IVersionedIndex
{
    public VersionedIndex(IElasticConfiguration configuration, string name, int version = 1)
        : base(configuration, name)
    {
        Version = version;
        VersionedName = String.Concat(Name, "-v", Version);
    }

    public int Version { get; }
    public string VersionedName { get; }
    public bool DiscardIndexesOnReindex { get; set; } = true;
    private List<ReindexScript> ReindexScripts { get; } = new List<ReindexScript>();

    private class ReindexScript
    {
        public int Version { get; set; }
        public string Script { get; set; }
    }

    protected virtual void AddReindexScript(int versionNumber, string script)
    {
        ReindexScripts.Add(new ReindexScript { Version = versionNumber, Script = script });
    }

    protected void RenameFieldScript(int versionNumber, string originalName, string currentName, bool removeOriginal = true)
    {
        string script = $"if (ctx._source.containsKey(\'{originalName}\')) {{ ctx._source[\'{currentName}\'] = ctx._source.{originalName}; }}";
        ReindexScripts.Add(new ReindexScript { Version = versionNumber, Script = script });

        if (removeOriginal)
            RemoveFieldScript(versionNumber, originalName);
    }

    protected void RemoveFieldScript(int versionNumber, string fieldName)
    {
        string script = $"if (ctx._source.containsKey(\'{fieldName}\')) {{ ctx._source.remove(\'{fieldName}\'); }}";
        ReindexScripts.Add(new ReindexScript { Version = versionNumber, Script = script });
    }

    public override async Task ConfigureAsync()
    {
        if (!await IndexExistsAsync(VersionedName).AnyContext())
        {
            if (!await AliasExistsAsync(Name).AnyContext())
            {
                await CreateIndexAsync(VersionedName, d =>
                {
                    ConfigureIndex(d);
                    d.Aliases(ad => ad.Add(Name, a => { }));
                }).AnyContext();
            }
            else // new version of an existing index, don't set the alias yet
                await CreateIndexAsync(VersionedName, ConfigureIndex).AnyContext();
        }
        else
        {
            await UpdateIndexAsync(VersionedName).AnyContext();
        }
    }

    protected override ElasticMappingResolver CreateMappingResolver()
    {
        return ElasticMappingResolver.Create(Configuration.Client, VersionedName, _logger);
    }

    protected virtual async Task CreateAliasAsync(string index, string name)
    {
        if (await AliasExistsAsync(name).AnyContext())
            return;

        var response = await Configuration.Client.Indices.UpdateAliasesAsync(a => a.Actions(actions => actions.Add(s => s.Index(index).Alias(name)))).AnyContext();
        if (response.IsValidResponse)
            return;

        if (await AliasExistsAsync(name).AnyContext())
            return;

        throw new RepositoryException(response.GetErrorMessage($"Error creating alias {name}"), response.OriginalException());
    }

    protected async Task<bool> AliasExistsAsync(string alias)
    {
        var response = await Configuration.Client.Indices.ExistsAliasAsync(Names.Parse(alias)).AnyContext();
        if (response.ApiCallDetails.HasSuccessfulStatusCode)
            return response.Exists;

        throw new RepositoryException(response.GetErrorMessage($"Error checking to see if alias {alias}"), response.OriginalException());
    }

    public override async Task DeleteAsync()
    {
        int currentVersion = await GetCurrentVersionAsync();
        var indexesToDelete = new List<string>();
        if (currentVersion != Version)
        {
            indexesToDelete.Add(String.Concat(Name, "-v", currentVersion));
            indexesToDelete.Add(String.Concat(Name, "-v", currentVersion, "-error"));
        }

        indexesToDelete.Add(VersionedName);
        indexesToDelete.Add(String.Concat(VersionedName, "-error"));
        await DeleteIndexesAsync(indexesToDelete.ToArray()).AnyContext();
    }

    public ReindexWorkItem CreateReindexWorkItem(int currentVersion)
    {
        var reindexWorkItem = new ReindexWorkItem
        {
            OldIndex = String.Concat(Name, "-v", currentVersion),
            NewIndex = VersionedName,
            Alias = Name,
            Script = GetReindexScripts(currentVersion),
            TimestampField = GetTimeStampField()
        };

        reindexWorkItem.DeleteOld = DiscardIndexesOnReindex && reindexWorkItem.OldIndex != reindexWorkItem.NewIndex;

        return reindexWorkItem;
    }

    private string GetReindexScripts(int currentVersion)
    {
        var scripts = ReindexScripts.Where(s => s.Version > currentVersion && Version >= s.Version).OrderBy(s => s.Version).ToList();
        if (scripts.Count == 0)
            return null;

        if (scripts.Count == 1)
            return scripts[0].Script;

        string fullScriptWithFunctions = String.Empty;
        string functionCalls = String.Empty;
        for (int i = 0; i < scripts.Count; i++)
        {
            var script = scripts[i];
            fullScriptWithFunctions += $"void f{i:000}(def ctx) {{ {script.Script} }}\r\n";
            functionCalls += $"f{i:000}(ctx); ";
        }

        return fullScriptWithFunctions + functionCalls;
    }

    public override async Task ReindexAsync(Func<int, string, Task> progressCallbackAsync = null)
    {
        int currentVersion = await GetCurrentVersionAsync().AnyContext();
        if (currentVersion < 0 || currentVersion >= Version)
            return;

        var reindexWorkItem = CreateReindexWorkItem(currentVersion);
        var reindexer = new ElasticReindexer(Configuration.Client, _logger);
        await reindexer.ReindexAsync(reindexWorkItem, progressCallbackAsync).AnyContext();
    }

    public override async Task MaintainAsync(bool includeOptionalTasks = true)
    {
        if (await AliasExistsAsync(Name).AnyContext())
            return;

        int currentVersion = await GetCurrentVersionAsync().AnyContext();
        if (currentVersion < 0)
            currentVersion = Version;

        await CreateAliasAsync(String.Concat(Name, "-v", currentVersion), Name).AnyContext();
    }

    /// <summary>
    /// Returns the current index version (E.G., the oldest index version).
    /// </summary>
    /// <returns>-1 if there are no indexes.</returns>
    public virtual async Task<int> GetCurrentVersionAsync()
    {
        int version = await GetVersionFromAliasAsync(Name).AnyContext();
        if (version >= 0)
            return version;

        var indexes = await GetIndexesAsync().AnyContext();
        if (indexes.Count == 0)
            return Version;

        return indexes.Select(i => i.Version).OrderBy(v => v).First();
    }

    protected virtual async Task<int> GetVersionFromAliasAsync(string alias)
    {
        var response = await Configuration.Client.Indices.GetAliasAsync(a => a.Name(Names.Parse(alias))).AnyContext();
        if (!response.IsValidResponse && response.ElasticsearchServerError?.Status == 404)
            return -1;

        // GetAliasResponse IS the dictionary (inherits from DictionaryResponse)
        var indices = response.Aliases;
        if (response.IsValidResponse && indices != null && indices.Count > 0)
        {
            _logger.LogRequest(response);
            return indices.Keys.Select(i => GetIndexVersion(i.ToString())).OrderBy(v => v).First();
        }

        _logger.LogErrorRequest(response, "Error getting index version from alias");
        return -1;
    }

    protected virtual int GetIndexVersion(string name)
    {
        if (String.IsNullOrEmpty(name))
            throw new ArgumentNullException(nameof(name));

        string namePrefix = $"{Name}-v";
        if (name.Length <= namePrefix.Length || !name.StartsWith(namePrefix))
            return -1;

        string input = name.Substring($"{Name}-v".Length);
        int index = input.IndexOf('-');
        if (index > 0)
            input = input.Substring(0, index);

        if (Int32.TryParse(input, out int version))
            return version;

        return -1;
    }

    protected virtual async Task<IList<IndexInfo>> GetIndexesAsync(int version = -1)
    {
        string filter = version < 0 ? $"{Name}-v*" : $"{Name}-v{version}";
        if (HasMultipleIndexes)
            filter += "-*";

        var sw = Stopwatch.StartNew();
        var response = await Configuration.Client.Indices.GetAsync((Indices)(IndexName)filter).AnyContext();
        sw.Stop();
        _logger.LogRequest(response);

        if (!response.IsValidResponse)
        {
            if (response.ElasticsearchServerError?.Status == 404)
                return new List<IndexInfo>();

            throw new RepositoryException(response.GetErrorMessage($"Error getting indices {filter}"), response.OriginalException());
        }

        if (response.Indices.Count == 0)
            return new List<IndexInfo>();

        var aliasResponse = await Configuration.Client.Indices.GetAliasAsync(a => a.Name($"{Name}-*")).AnyContext();
        _logger.LogRequest(aliasResponse);

        if (!aliasResponse.IsValidResponse && aliasResponse.ElasticsearchServerError?.Status != 404)
            throw new RepositoryException(response.GetErrorMessage($"Error getting index aliases for {filter}"), response.OriginalException());

        var aliasIndices = aliasResponse.Aliases;
        var indices = response.Indices.Keys
            .Where(i => version < 0 || GetIndexVersion(i.ToString()) == version)
            .Select(i =>
            {
                string indexName = i.ToString();
                var indexDate = GetIndexDate(indexName);
                string indexAliasName = GetIndexByDate(GetIndexDate(indexName));

                int currentVersion = -1;
                if (aliasResponse.IsValidResponse && aliasIndices != null && aliasIndices.TryGetValue(i, out var indexAliases))
                {
                    // Find if any of our aliases point to this index
                    if (indexAliases.Aliases.ContainsKey(indexAliasName))
                        currentVersion = GetIndexVersion(indexName);
                }

                return new IndexInfo { DateUtc = indexDate, Index = indexName, Version = GetIndexVersion(indexName), CurrentVersion = currentVersion };
            })
            .OrderBy(i => i.DateUtc)
            .ToList();

        _logger.LogInformation("Retrieved list of {IndexCount} indexes in {Duration:g}", indices.Count, sw.Elapsed);
        return indices;
    }

    protected virtual DateTime GetIndexDate(string name)
    {
        return DateTime.MaxValue;
    }

    protected virtual string GetIndexByDate(DateTime date)
    {
        return Name;
    }

    [DebuggerDisplay("{Index} (Date: {DateUtc} Version: {Version} CurrentVersion: {CurrentVersion})")]
    protected class IndexInfo
    {
        public string Index { get; set; }
        public int Version { get; set; }
        public int CurrentVersion { get; set; } = -1;
        public DateTime DateUtc { get; set; }
    }
}

public class VersionedIndex<T> : VersionedIndex, IIndex<T> where T : class
{
    private readonly string _typeName = typeof(T).Name.ToLower();

    public VersionedIndex(IElasticConfiguration configuration, string name = null, int version = 1) : base(configuration, name, version)
    {
        Name = name ?? _typeName;
    }

    protected override ElasticMappingResolver CreateMappingResolver()
    {
        return ElasticMappingResolver.Create<T>(ConfigureIndexMapping, Configuration.Client, VersionedName, _logger);
    }

    public virtual void ConfigureIndexMapping(TypeMappingDescriptor<T> map)
    {
        map.Properties(p => p.SetupDefaults());
    }

    public override void ConfigureIndex(CreateIndexRequestDescriptor idx)
    {
        base.ConfigureIndex(idx);
        idx.Mappings<T>(f =>
        {
            if (CustomFieldTypes.Count > 0)
            {
                f.DynamicTemplates(d =>
                {
                    foreach (var customFieldType in CustomFieldTypes.Values)
                        d.Add($"idx_{customFieldType.Type}", df => df.PathMatch("idx.*").Match($"{customFieldType.Type}-*").Mapping(customFieldType.ConfigureMapping<T>()));
                });
            }

            ConfigureIndexMapping(f);
        });
    }

    protected override async Task UpdateIndexAsync(string name, Action<PutIndicesSettingsRequestDescriptor> descriptor = null)
    {
        await base.UpdateIndexAsync(name, descriptor).AnyContext();

        var typeMappingDescriptor = new TypeMappingDescriptor<T>();
        ConfigureIndexMapping(typeMappingDescriptor);
        var mapping = (TypeMapping)typeMappingDescriptor;

        var response = await Configuration.Client.Indices.PutMappingAsync<T>(m =>
        {
            m.Indices(name);
            m.Properties(mapping.Properties);
            if (CustomFieldTypes.Count > 0)
            {
                m.DynamicTemplates(d =>
                {
                    foreach (var customFieldType in CustomFieldTypes.Values)
                        d.Add($"idx_{customFieldType.Type}", df => df.PathMatch("idx.*").Match($"{customFieldType.Type}-*").Mapping(customFieldType.ConfigureMapping<T>()));
                });
            }
        }).AnyContext();

        // TODO: Check for issues with attempting to change existing fields and warn that index version needs to be incremented
        if (response.IsValidResponse)
            _logger.LogRequest(response);
        else
            _logger.LogErrorRequest(response, $"Error updating index ({name}) mappings. Changing existing fields requires a new index version.");
    }

    public override void ConfigureSettings(ElasticsearchClientSettings settings)
    {
        settings.DefaultMappingFor<T>(d => d.IndexName(Name));
    }

    protected override string GetTimeStampField()
    {
        if (typeof(IHaveDates).IsAssignableFrom(typeof(T)))
            return InferField(f => ((IHaveDates)f).UpdatedUtc);

        if (typeof(IHaveCreatedDate).IsAssignableFrom(typeof(T)))
            return InferField(f => ((IHaveCreatedDate)f).CreatedUtc);

        return null;
    }

    public Inferrer Infer => Configuration.Client.Infer;
    public string InferField(Expression<Func<T, object>> objectPath) => Infer.Field(objectPath);
    public string InferPropertyName(Expression<Func<T, object>> objectPath) => Infer.PropertyName(objectPath);
}
