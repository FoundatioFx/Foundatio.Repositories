using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Elasticsearch.Net;
using Foundatio.Caching;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.CustomFields;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;
using Foundatio.Repositories.Queries;
using Foundatio.Utility;
using Microsoft.Extensions.Logging;
using Nest;

namespace Foundatio.Repositories.Elasticsearch;

public abstract class ElasticReadOnlyRepositoryBase<T> : ISearchableReadOnlyRepository<T> where T : class, new() {
    protected static readonly bool HasIdentity = typeof(IIdentity).IsAssignableFrom(typeof(T));
    protected static readonly bool HasDates = typeof(IHaveDates).IsAssignableFrom(typeof(T));
    protected static readonly bool HasCreatedDate = typeof(IHaveCreatedDate).IsAssignableFrom(typeof(T));
    protected static readonly bool SupportsSoftDeletes = typeof(ISupportSoftDeletes).IsAssignableFrom(typeof(T));
    protected static readonly bool HasVersion = typeof(IVersioned).IsAssignableFrom(typeof(T));
    protected static readonly bool HasCustomFields = typeof(IHaveCustomFields).IsAssignableFrom(typeof(T));
    protected static readonly string EntityTypeName = typeof(T).Name;
    protected static readonly IReadOnlyCollection<T> EmptyList = new List<T>(0).AsReadOnly();
    private readonly List<Lazy<Field>> _defaultExcludes = new();
    protected readonly Lazy<string> _idField;

    protected readonly ILogger _logger;
    protected readonly Lazy<IElasticClient> _lazyClient;
    protected IElasticClient _client => _lazyClient.Value;

    private ScopedCacheClient _scopedCacheClient;

    protected ElasticReadOnlyRepositoryBase(IIndex index) {
        ElasticIndex = index;
        if (HasIdentity)
            _idField = new Lazy<string>(() => InferField(d => ((IIdentity)d).Id) ?? "id");
        _lazyClient = new Lazy<IElasticClient>(() => index.Configuration.Client);

        SetCacheClient(index.Configuration.Cache);
        _logger = index.Configuration.LoggerFactory.CreateLogger(GetType());
    }

    protected IIndex ElasticIndex { get; private set; }
    protected Func<T, string> GetParentIdFunc { get; set; }

    protected Inferrer Infer => ElasticIndex.Configuration.Client.Infer;
    protected string InferField(Expression<Func<T, object>> objectPath) => Infer.Field(objectPath);
    protected string InferPropertyName(Expression<Func<T, object>> objectPath) => Infer.PropertyName(objectPath);
    protected bool HasParent { get; set; } = false;

    protected Consistency DefaultConsistency { get; set; } = Consistency.Eventual;
    protected TimeSpan DefaultCacheExpiration { get; set; } = TimeSpan.FromSeconds(60 * 5);
    protected int DefaultPageLimit { get; set; } = 10;
    protected int MaxPageLimit { get; set; } = 10000;
    protected Microsoft.Extensions.Logging.LogLevel DefaultQueryLogLevel { get; set; } = Microsoft.Extensions.Logging.LogLevel.Trace;

    #region IReadOnlyRepository

    public Task<T> GetByIdAsync(Id id, CommandOptionsDescriptor<T> options) {
        return GetByIdAsync(id, options.Configure());
    }

    public virtual async Task<T> GetByIdAsync(Id id, ICommandOptions options = null) {
        if (String.IsNullOrEmpty(id.Value))
            return null;

        options = ConfigureOptions(options.As<T>());

        await OnBeforeGetAsync(new Ids(id), options, typeof(T));

        // we don't have the parent id so we have to do a query
        if (HasParent && id.Routing == null) {
            var result = await FindOneAsync(NewQuery().Id(id), options).AnyContext();
            return result?.Document;
        }

        if (IsCacheEnabled && options.ShouldReadCache()) {
            var value = await GetCachedFindHit(id).AnyContext();

            if (value?.Document != null) {
                _logger.LogTrace("Cache hit: type={EntityType} key={Id}", EntityTypeName, id);

                return ShouldReturnDocument(value.Document, options) ? value.Document : null;
            }
        }

        var request = new GetRequest(ElasticIndex.GetIndex(id), id.Value);
        if (id.Routing != null)
            request.Routing = id.Routing;
        var response = await _client.GetAsync<T>(request).AnyContext();
        _logger.LogRequest(response, options.GetQueryLogLevel());

        var findHit = response.Found ? response.ToFindHit() : null;

        if (IsCacheEnabled && options.ShouldUseCache())
            await AddDocumentsToCacheAsync(findHit ?? new FindHit<T>(id, null, 0), options, false).AnyContext();
        
        return ShouldReturnDocument(findHit?.Document, options) ? findHit?.Document : null;
    }

    public Task<IReadOnlyCollection<T>> GetByIdsAsync(Ids ids, CommandOptionsDescriptor<T> options) {
        return GetByIdsAsync(ids, options.Configure());
    }

    public virtual async Task<IReadOnlyCollection<T>> GetByIdsAsync(Ids ids, ICommandOptions options = null) {
        var idList = ids?.Distinct().Where(i => !String.IsNullOrEmpty(i)).ToList();
        if (idList == null || idList.Count == 0)
            return EmptyList;

        if (!HasIdentity)
            throw new NotSupportedException("Model type must implement IIdentity.");

        options = ConfigureOptions(options.As<T>());

        await OnBeforeGetAsync(new Ids(idList), options, typeof(T));

        var hits = new List<FindHit<T>>();
        if (IsCacheEnabled && options.ShouldReadCache())
            hits.AddRange(await GetCachedFindHit(idList).AnyContext());

        var itemsToFind = idList.Except(hits.Select(i => (Id)i.Id)).ToList();
        if (itemsToFind.Count == 0)
            return hits.Where(h => h.Document != null && ShouldReturnDocument(h.Document, options)).Select(h => h.Document).ToList().AsReadOnly();

        var multiGet = new MultiGetDescriptor();
        foreach (var id in itemsToFind.Where(i => i.Routing != null || !HasParent)) {
            multiGet.Get<T>(f => {
                f.Id(id.Value).Index(ElasticIndex.GetIndex(id));
                if (id.Routing != null)
                    f.Routing(id.Routing);

                return f;
            });
        }

        var multiGetResults = await _client.MultiGetAsync(multiGet).AnyContext();
        _logger.LogRequest(multiGetResults, options.GetQueryLogLevel());

        foreach (var doc in multiGetResults.Hits) {
            hits.Add(((IMultiGetHit<T>)doc).ToFindHit());
            itemsToFind.Remove(new Id(doc.Id, doc.Routing));
        }

        // fallback to doing a find
        if (itemsToFind.Count > 0 && (HasParent || ElasticIndex.HasMultipleIndexes)) {
            var response = await FindAsync(q => q.Id(itemsToFind.Select(id => id.Value)), o => o.PageLimit(1000)).AnyContext();
            do {
                if (response.Hits.Count > 0)
                    hits.AddRange(response.Hits.Where(h => h.Document != null));
            } while (await response.NextPageAsync().AnyContext());
        }

        if (IsCacheEnabled && options.ShouldUseCache())
            await AddDocumentsToCacheAsync(hits, options, false).AnyContext();

        return hits.Where(h => h.Document != null && ShouldReturnDocument(h.Document, options)).Select(h => h.Document).ToList().AsReadOnly();
    }

    public Task<FindResults<T>> GetAllAsync(CommandOptionsDescriptor<T> options) {
        return GetAllAsync(options.Configure());
    }

    public virtual Task<FindResults<T>> GetAllAsync(ICommandOptions options = null) {
        return FindAsync(NewQuery(), options);
    }

    public Task<bool> ExistsAsync(Id id, CommandOptionsDescriptor<T> options) {
        return ExistsAsync(id, options.Configure());
    }

    public virtual async Task<bool> ExistsAsync(Id id, ICommandOptions options = null) {
        if (String.IsNullOrEmpty(id.Value))
            return false;

        // documents that use soft deletes or have parents without a routing id need to use search for exists
        if (!SupportsSoftDeletes && (!HasParent || id.Routing != null)) {
            var response = await _client.DocumentExistsAsync(new DocumentPath<T>(id.Value), d => {
                d.Index(ElasticIndex.GetIndex(id));
                if (id.Routing != null)
                    d.Routing(id.Routing);

                return d;
            }).AnyContext();
            _logger.LogRequest(response, options.GetQueryLogLevel());

            return response.Exists;
        }

        return await ExistsAsync(q => q.Id(id), o => options.As<T>()).AnyContext();
    }

    public Task<CountResult> CountAsync(CommandOptionsDescriptor<T> options) {
        return CountAsync(options.Configure());
    }

    public virtual async Task<CountResult> CountAsync(ICommandOptions options = null) {
        var result = await CountAsync(NewQuery(), options).AnyContext();
        return result;
    }

    public Task InvalidateCacheAsync(T document) {
        return InvalidateCacheAsync(new[] { document });
    }

    public Task InvalidateCacheAsync(IEnumerable<T> documents) {
        var docs = documents?.ToList();
        if (docs == null || docs.Any(d => d == null))
            throw new ArgumentNullException(nameof(documents));

        if (!IsCacheEnabled)
            return Task.CompletedTask;

        return InvalidateCacheAsync(docs.Select(d => new ModifiedDocument<T>(d, null)).ToList());
    }

    public Task InvalidateCacheAsync(string cacheKey) {
        return InvalidateCacheAsync(new[] { cacheKey });
    }

    public Task InvalidateCacheAsync(IEnumerable<string> cacheKeys) {
        var keys = cacheKeys?.ToList();
        if (keys == null || keys.Any(k => k == null))
            throw new ArgumentNullException(nameof(cacheKeys));

        if (keys.Count > 0)
            return Cache.RemoveAllAsync(keys);

        return Task.CompletedTask;
    }

    public AsyncEvent<BeforeGetEventArgs<T>> BeforeGet { get; } = new AsyncEvent<BeforeGetEventArgs<T>>();

    private async Task OnBeforeGetAsync(Ids ids, ICommandOptions options, Type resultType) {
        if (BeforeGet == null || !BeforeGet.HasHandlers)
            return;

        await BeforeGet.InvokeAsync(this, new BeforeGetEventArgs<T>(ids, options, this, resultType)).AnyContext();
    }

    public AsyncEvent<BeforeQueryEventArgs<T>> BeforeQuery { get; } = new AsyncEvent<BeforeQueryEventArgs<T>>();

    private async Task OnBeforeQueryAsync(IRepositoryQuery query, ICommandOptions options, Type resultType) {
        if (SupportsSoftDeletes && IsCacheEnabled && options.GetSoftDeleteMode() == SoftDeleteQueryMode.ActiveOnly) {
            var deletedIds = await Cache.GetListAsync<string>("deleted").AnyContext();
            if (deletedIds.HasValue)
                query.ExcludedId(deletedIds.Value);
        }

        var systemFilter = query.GetSystemFilter();
        if (systemFilter != null)
            query.MergeFrom(systemFilter.GetQuery());

        if (BeforeQuery == null || !BeforeQuery.HasHandlers)
            return;

        await BeforeQuery.InvokeAsync(this, new BeforeQueryEventArgs<T>(query, options, this, resultType)).AnyContext();
    }

    #endregion

    #region ISearchableReadOnlyRepository

    public virtual Task<FindResults<T>> FindAsync(RepositoryQueryDescriptor<T> query, CommandOptionsDescriptor<T> options = null) {
        return FindAsync(query.Configure(), options.Configure());
    }

    public Task<FindResults<T>> FindAsync(IRepositoryQuery query, ICommandOptions options = null) {
        return FindAsAsync<T>(query, options);
    }

    public Task<FindResults<TResult>> FindAsAsync<TResult>(RepositoryQueryDescriptor<T> query, CommandOptionsDescriptor<T> options = null) where TResult : class, new() {
        return FindAsAsync<TResult>(query.Configure(), options.Configure());
    }

    public virtual async Task<FindResults<TResult>> FindAsAsync<TResult>(IRepositoryQuery query, ICommandOptions options = null) where TResult : class, new() {
        options = ConfigureOptions(options.As<T>());
        bool useSnapshotPaging = options.ShouldUseSnapshotPaging();
        // don't use caching with snapshot paging.
        bool allowCaching = IsCacheEnabled && useSnapshotPaging == false;

        await OnBeforeQueryAsync(query, options, typeof(TResult)).AnyContext();

        await RefreshForConsistency(query, options).AnyContext();

        string cacheSuffix = options?.HasPageLimit() == true ? String.Concat(options.GetPage().ToString(), ":", options.GetLimit().ToString()) : null;

        FindResults<TResult> result;
        if (allowCaching) {
            result = await GetCachedQueryResultAsync<FindResults<TResult>>(options, cacheSuffix: cacheSuffix).AnyContext();
            if (result != null) {
                ((IFindResults<TResult>)result).GetNextPageFunc = async previousResults => await GetNextPageFunc(previousResults, query, options).AnyContext();
                return result;
            }
        }

        if (options.HasAsyncQueryId()) {
            var queryId = options.GetAsyncQueryId();
            if (String.IsNullOrEmpty(queryId))
                throw new ArgumentNullException("AsyncQueryId must not be null");

            var response = await _client.AsyncSearch.GetAsync<TResult>(queryId, s => {
                if (options.HasAsyncQueryWaitTime())
                    s.WaitForCompletionTimeout(options.GetAsyncQueryWaitTime());
                return s;
            }).AnyContext();
            
            if (options.ShouldAutoDeleteAsyncQuery() && !response.IsRunning)
                await RemoveQueryAsync(queryId);

            _logger.LogRequest(response, options.GetQueryLogLevel());
            if (!response.IsValid && response.ApiCall.HttpStatusCode.GetValueOrDefault() == 404)
                throw new AsyncQueryNotFoundException(queryId);

            result = response.ToFindResults(options);
        } else if (options.HasSnapshotScrollId()) {
            var response = await _client.ScrollAsync<TResult>(options.GetSnapshotLifetime(), options.GetSnapshotScrollId()).AnyContext();
            _logger.LogRequest(response, options.GetQueryLogLevel());
            result = response.ToFindResults(options);
        } else {
            var searchDescriptor = await CreateSearchDescriptorAsync(query, options).AnyContext();
            if (useSnapshotPaging)
                searchDescriptor.Scroll(options.GetSnapshotLifetime());

            if (query.ShouldOnlyHaveIds())
                searchDescriptor.Source(false);

            if (options.ShouldUseAsyncQuery()) {
                var asyncSearchDescriptor = searchDescriptor.ToAsyncSearchSubmitDescriptor();

                if (options.HasAsyncQueryWaitTime())
                    asyncSearchDescriptor.WaitForCompletionTimeout(options.GetAsyncQueryWaitTime());

                var response = await _client.AsyncSearch.SubmitAsync<TResult>(asyncSearchDescriptor).AnyContext();
                _logger.LogRequest(response, options.GetQueryLogLevel());
                result = response.ToFindResults(options);
            } else {
                var response = await _client.SearchAsync<TResult>(searchDescriptor).AnyContext();
                _logger.LogRequest(response, options.GetQueryLogLevel());
                result = response.ToFindResults(options);
            }
        }

        if (useSnapshotPaging && !result.HasMore) {
            // clear the scroll
            string scrollId = result.GetScrollId();
            if (!String.IsNullOrEmpty(scrollId))
                await _client.ClearScrollAsync(s => s.ScrollId(result.GetScrollId()));
        }

        if (allowCaching && !result.IsAsyncQueryRunning() && !result.IsAsyncQueryPartial())
            await SetCachedQueryResultAsync(options, result, cacheSuffix: cacheSuffix).AnyContext();
        
        ((IFindResults<TResult>)result).GetNextPageFunc = previousResults => GetNextPageFunc(previousResults, query, options);

        return result;
    }

    public Task RemoveQueryAsync(string queryId) {
        return _client.AsyncSearch.DeleteAsync(queryId);
    }

    private async Task<FindResults<TResult>> GetNextPageFunc<TResult>(FindResults<TResult> previousResults, IRepositoryQuery query, ICommandOptions options) where TResult : class, new() {
        if (previousResults == null)
            throw new ArgumentException(nameof(previousResults));

        string scrollId = previousResults.GetScrollId();
        if (!String.IsNullOrEmpty(scrollId)) {
            var scrollResponse = await _client.ScrollAsync<TResult>(options.GetSnapshotLifetime(), scrollId).AnyContext();
            _logger.LogRequest(scrollResponse, options.GetQueryLogLevel());

            var results = scrollResponse.ToFindResults(options);
            ((IFindResults<T>)results).Page = previousResults.Page + 1;

            // clear the scroll
            if (!results.HasMore)
                await _client.ClearScrollAsync(s => s.ScrollId(scrollId));

            return results;
        }

        if (options.ShouldUseSearchAfterPaging())
            options.SearchAfterToken(previousResults.GetSearchAfterToken());

        if (options == null)
            return new FindResults<TResult>();

        options?.PageNumber(!options.HasPageNumber() ? 2 : options.GetPage() + 1);
        return await FindAsAsync<TResult>(query, options).AnyContext();
    }

    public Task<FindHit<T>> FindOneAsync(RepositoryQueryDescriptor<T> query, CommandOptionsDescriptor<T> options = null) {
        return FindOneAsync(query.Configure(), options.Configure());
    }

    public virtual async Task<FindHit<T>> FindOneAsync(IRepositoryQuery query, ICommandOptions options = null) {
        options = ConfigureOptions(options.As<T>());
        if (IsCacheEnabled && (options.ShouldUseCache() || options.ShouldReadCache()) && !options.HasCacheKey())
            throw new ArgumentException("Cache key is required when enabling cache.", nameof(options));

        var result = IsCacheEnabled && options.ShouldReadCache() && options.HasCacheKey() ? await GetCachedFindHit(options).AnyContext() : null;
        if (result != null)
            return result.FirstOrDefault();

        await OnBeforeQueryAsync(query, options, typeof(T)).AnyContext();

        await RefreshForConsistency(query, options).AnyContext();

        var searchDescriptor = await CreateSearchDescriptorAsync(query, options).AnyContext();
        searchDescriptor.Size(1);
        var response = await _client.SearchAsync<T>(searchDescriptor).AnyContext();

        if (response.IsValid) {
            _logger.LogRequest(response, options.GetQueryLogLevel());
        } else {
            if (response.ApiCall.HttpStatusCode.GetValueOrDefault() == 404)
                return FindHit<T>.Empty;

            throw new DocumentException(response.GetErrorMessage("Error while finding document"), response.OriginalException);
        }

        result = response.Hits.Select(h => h.ToFindHit()).ToList();

        if (IsCacheEnabled && options.ShouldUseCache())
            await AddDocumentsToCacheAsync(result, options, options.GetConsistency(DefaultConsistency) == Consistency.Eventual).AnyContext();

        return result.FirstOrDefault();
    }

    public Task<CountResult> CountAsync(RepositoryQueryDescriptor<T> query, CommandOptionsDescriptor<T> options = null) {
        return CountAsync(query.Configure(), options.Configure());
    }

    public virtual async Task<CountResult> CountAsync(IRepositoryQuery query, ICommandOptions options = null) {
        options = ConfigureOptions(options.As<T>());

        CountResult result;
        if (IsCacheEnabled && options.ShouldReadCache()) {
            result = await GetCachedQueryResultAsync<CountResult>(options, "count").AnyContext();
            if (result != null)
                return result;
        }

        await OnBeforeQueryAsync(query, options, typeof(T)).AnyContext();

        await RefreshForConsistency(query, options).AnyContext();

        var searchDescriptor = await CreateSearchDescriptorAsync(query, options).AnyContext();
        searchDescriptor.Size(0);

        if (options.HasAsyncQueryId()) {
            var queryId = options.GetAsyncQueryId();
            if (String.IsNullOrEmpty(queryId))
                throw new ArgumentNullException("AsyncQueryId must not be null");

            var response = await _client.AsyncSearch.GetAsync<T>(queryId, s => {
                if (options.HasAsyncQueryWaitTime())
                    s.WaitForCompletionTimeout(options.GetAsyncQueryWaitTime());
                return s;
            }).AnyContext();

            if (options.ShouldAutoDeleteAsyncQuery() && !response.IsRunning)
                await RemoveQueryAsync(queryId);

            _logger.LogRequest(response, options.GetQueryLogLevel());
            result = response.ToCountResult(options);
        } else if(options.ShouldUseAsyncQuery()) {
            var asyncSearchDescriptor = searchDescriptor.ToAsyncSearchSubmitDescriptor();

            if (options.HasAsyncQueryWaitTime())
                asyncSearchDescriptor.WaitForCompletionTimeout(options.GetAsyncQueryWaitTime());

            var response = await _client.AsyncSearch.SubmitAsync<T>(asyncSearchDescriptor).AnyContext();
            _logger.LogRequest(response, options.GetQueryLogLevel());
            result = response.ToCountResult(options);
        } else {
            var response = await _client.SearchAsync<T>(searchDescriptor).AnyContext();
            result = response.ToCountResult(options);
        }

        if (IsCacheEnabled && options.ShouldUseCache() && !result.IsAsyncQueryRunning() && !result.IsAsyncQueryPartial())
            await SetCachedQueryResultAsync(options, result, "count").AnyContext();

        return result;
    }

    public Task<bool> ExistsAsync(RepositoryQueryDescriptor<T> query, CommandOptionsDescriptor<T> options = null) {
        return ExistsAsync(query.Configure(), options.Configure());
    }

    public virtual async Task<bool> ExistsAsync(IRepositoryQuery query, ICommandOptions options = null) {
        options = ConfigureOptions(options.As<T>());
        await OnBeforeQueryAsync(query, options, typeof(T)).AnyContext();

        await RefreshForConsistency(query, options).AnyContext();

        var searchDescriptor = (await CreateSearchDescriptorAsync(query, options).AnyContext()).Size(0);
        searchDescriptor.DocValueFields(_idField.Value);
        var response = await _client.SearchAsync<T>(searchDescriptor).AnyContext();

        if (response.IsValid) {
            _logger.LogRequest(response, options.GetQueryLogLevel());
        } else {
            if (response.ApiCall.HttpStatusCode.GetValueOrDefault() == 404)
                return false;

            throw new DocumentException(response.GetErrorMessage("Error checking if document exists"), response.OriginalException);
        }

        return response.Total > 0;
    }

    public virtual Task<FindResults<T>> SearchAsync(ISystemFilter systemFilter, string filter = null, string criteria = null, string sort = null, string aggregations = null, ICommandOptions options = null) {
        return FindAsAsync<T>(q => q.SystemFilter(systemFilter).FilterExpression(filter).SearchExpression(criteria).SortExpression(sort).AggregationsExpression(aggregations), o => options.As<T>());
    }

    public virtual Task<CountResult> CountBySearchAsync(ISystemFilter systemFilter, string filter = null, string aggregations = null, ICommandOptions options = null) {
        return CountAsync(q => q.SystemFilter(systemFilter).FilterExpression(filter).AggregationsExpression(aggregations), o => options.As<T>());
    }

    #endregion

    protected virtual IRepositoryQuery<T> NewQuery() {
        return new RepositoryQuery<T>();
    }

    protected virtual IRepositoryQuery ConfigureQuery(IRepositoryQuery<T> query) {
        if (query == null)
            query = new RepositoryQuery<T>();

        if (_defaultExcludes.Count > 0 && query.GetExcludes().Count == 0)
            query.Exclude(_defaultExcludes.Select(e => e.Value));

        return query;
    }

    protected virtual Task InvalidateCacheByQueryAsync(IRepositoryQuery<T> query) {
        return InvalidateCacheAsync(query.GetIds());
    }

    protected void AddDefaultExclude(string field) {
        _defaultExcludes.Add(new Lazy<Field>(() => field));
    }
    
    protected void AddDefaultExclude(Lazy<string> field) {
        _defaultExcludes.Add(new Lazy<Field>(() => field.Value));
    }
    
    protected void AddDefaultExclude(Expression<Func<T, object>> objectPath) {
        _defaultExcludes.Add(new Lazy<Field>(() => InferPropertyName(objectPath)));
    }
    
    protected void AddDefaultExclude(params Expression<Func<T, object>>[] objectPaths) {
        _defaultExcludes.AddRange(objectPaths.Select(o => new Lazy<Field>(() => InferPropertyName(o))));
    }

    public bool IsCacheEnabled { get; private set; } = false;
    protected ScopedCacheClient Cache => _scopedCacheClient ?? new ScopedCacheClient(new NullCacheClient());

    private void SetCacheClient(ICacheClient cache) {
        IsCacheEnabled = cache != null && !(cache is NullCacheClient);
        _scopedCacheClient = new ScopedCacheClient(cache ?? new NullCacheClient(), EntityTypeName);
    }

    protected void DisableCache() {
        IsCacheEnabled = false;
        _scopedCacheClient = new ScopedCacheClient(new NullCacheClient(), EntityTypeName);
    }

    protected virtual Task InvalidateCacheAsync(IReadOnlyCollection<ModifiedDocument<T>> documents, ChangeType? changeType = null) {
        var keysToRemove = new HashSet<string>();

        if (HasIdentity && changeType != ChangeType.Added) {
            foreach (var document in documents) {
                keysToRemove.Add(((IIdentity)document.Value).Id);
                if (((IIdentity)document.Original)?.Id != null)
                    keysToRemove.Add(((IIdentity)document.Original).Id);
            }
        }

        if (keysToRemove.Count > 0)
            return Cache.RemoveAllAsync(keysToRemove);
        
        return Task.CompletedTask;
    }

    protected virtual Task<SearchDescriptor<T>> CreateSearchDescriptorAsync(IRepositoryQuery query, ICommandOptions options) {
        return ConfigureSearchDescriptorAsync(null, query, options);
    }

    protected virtual async Task<SearchDescriptor<T>> ConfigureSearchDescriptorAsync(SearchDescriptor<T> search, IRepositoryQuery query, ICommandOptions options) {
        search ??= new SearchDescriptor<T>();

        query = ConfigureQuery(query.As<T>()).Unwrap();
        string[] indices = ElasticIndex.GetIndexesByQuery(query);
        if (indices?.Length > 0)
            search.Index(String.Join(",", indices));
        if (HasVersion)
            search.SequenceNumberPrimaryTerm(HasVersion);

        if (options.HasQueryTimeout())
            search.Timeout(new Time(options.GetQueryTimeout()).ToString());

        search.IgnoreUnavailable();
        search.TrackTotalHits();

        await ElasticIndex.QueryBuilder.ConfigureSearchAsync(query, options, search).AnyContext();

        return search;
    }

    protected virtual ICommandOptions<T> ConfigureOptions(ICommandOptions<T> options) {
        options ??= new CommandOptions<T>();

        options.ElasticIndex(ElasticIndex);
        options.SupportsSoftDeletes(SupportsSoftDeletes);
        options.DocumentType(typeof(T));
        options.DefaultCacheExpiresIn(DefaultCacheExpiration);
        options.DefaultPageLimit(DefaultPageLimit);
        options.MaxPageLimit(MaxPageLimit);
        options.DefaultQueryLogLevel(DefaultQueryLogLevel);

        return options;
    }

    private bool ShouldReturnDocument(T document, ICommandOptions options) {
        if (document == null)
            return true;

        if (!SupportsSoftDeletes)
            return true;

        var mode = options.GetSoftDeleteMode();
        bool returnSoftDeletes = mode == SoftDeleteQueryMode.All || mode == SoftDeleteQueryMode.DeletedOnly;
        return returnSoftDeletes || !((ISupportSoftDeletes)document).IsDeleted;
    }

    private Task RefreshForConsistency(IRepositoryQuery query, ICommandOptions options) {
        // all docs are saved with immediate or wait consistency, no need to force a refresh
        if (DefaultConsistency != Consistency.Eventual)
            return Task.CompletedTask;

        // if using immediate consistency, force a refresh before query
        if (options.GetConsistency() == Consistency.Immediate) {
            string[] indices = ElasticIndex.GetIndexesByQuery(query);
            return _client.Indices.RefreshAsync(indices);
        }

        return Task.CompletedTask;
    }

    protected async Task<TResult> GetCachedQueryResultAsync<TResult>(ICommandOptions options, string cachePrefix = null, string cacheSuffix = null) {
        if (IsCacheEnabled && (options.ShouldUseCache() || options.ShouldReadCache()) && !options.HasCacheKey())
            throw new ArgumentException("Cache key is required when enabling cache.", nameof(options));

        if (!IsCacheEnabled || !options.ShouldReadCache() || !options.HasCacheKey())
            return default;

        string cacheKey = cachePrefix != null ? cachePrefix + ":" + options.GetCacheKey() : options.GetCacheKey();
        if (!String.IsNullOrEmpty(cacheSuffix))
            cacheKey += ":" + cacheSuffix;

        var result = await Cache.GetAsync<TResult>(cacheKey, default).AnyContext();
        if (_logger.IsEnabled(Microsoft.Extensions.Logging.LogLevel.Trace))
            _logger.LogTrace("Cache {HitOrMiss}: type={EntityType} key={CacheKey}", (result != null ? "hit" : "miss"), EntityTypeName, cacheKey);

        return result;
    }

    protected async Task SetCachedQueryResultAsync<TResult>(ICommandOptions options, TResult result, string cachePrefix = null, string cacheSuffix = null) {
        if (!IsCacheEnabled || result == null || !options.ShouldUseCache())
            return;
        
        if (!options.HasCacheKey())
            throw new ArgumentException("Cache key is required when enabling cache.", nameof(options));

        string cacheKey = cachePrefix != null ? cachePrefix + ":" + options.GetCacheKey() : options.GetCacheKey();
        if (!String.IsNullOrEmpty(cacheSuffix))
            cacheKey += ":" + cacheSuffix;

        await Cache.SetAsync(cacheKey, result, options.GetExpiresIn()).AnyContext();
        if (_logger.IsEnabled(Microsoft.Extensions.Logging.LogLevel.Trace))
            _logger.LogTrace("Set cache: type={EntityType} key={CacheKey}", EntityTypeName, cacheKey);
    }

    protected async Task<ICollection<FindHit<T>>> GetCachedFindHit(ICommandOptions options) {
        string cacheKey = options.GetCacheKey();
        try {
            var cacheKeyHits = await Cache.GetAsync<ICollection<FindHit<T>>>(cacheKey).AnyContext();

            var result = cacheKeyHits.HasValue && !cacheKeyHits.IsNull ? cacheKeyHits.Value : null;

            if (_logger.IsEnabled(Microsoft.Extensions.Logging.LogLevel.Trace))
                _logger.LogTrace("Cache {HitOrMiss}: type={EntityType} key={CacheKey}", (result != null ? "hit" : "miss"), EntityTypeName, cacheKey);

            return result;
        } catch (Exception ex) {
            _logger.LogError(ex, "Error getting cached find hit: type={EntityType} key={CacheKey}", EntityTypeName, cacheKey);

            return null;
        }
    }
    
    protected async Task<FindHit<T>> GetCachedFindHit(Id id, string cacheKey = null) {
        try {
            var cacheKeyHits = await Cache.GetAsync<ICollection<FindHit<T>>>(cacheKey ?? id).AnyContext();
        
            var result = cacheKeyHits.HasValue && !cacheKeyHits.IsNull
                ? cacheKeyHits.Value.FirstOrDefault(v => v?.Document != null && String.Equals(v.Id, id))
                : null;
        
            if (_logger.IsEnabled(Microsoft.Extensions.Logging.LogLevel.Trace))
                _logger.LogTrace("Cache {HitOrMiss}: type={EntityType} key={CacheKey}", (result != null ? "hit" : "miss"), EntityTypeName, cacheKey ?? id);

            return result;
        } catch (Exception ex) {
            _logger.LogError(ex, "Error getting cached find hit: type={EntityType} key={CacheKey}", EntityTypeName, cacheKey ?? id);

            return null;
        }
    }

    protected async Task<ICollection<FindHit<T>>> GetCachedFindHit(ICollection<Id> ids, string cacheKey = null) {
        var idList = ids.Select(id => id.Value).ToList();
        IEnumerable<FindHit<T>> result;

        try {
            if (String.IsNullOrEmpty(cacheKey)) {
                var cacheHitsById = await Cache.GetAllAsync<ICollection<FindHit<T>>>(idList).AnyContext();
                result = cacheHitsById
                    .Where(kvp => kvp.Value.HasValue && !kvp.Value.IsNull)
                    .SelectMany(kvp => kvp.Value.Value)
                    .Where(v => v?.Document != null && idList.Contains(v.Id));
            } else {
                var cacheKeyHits = await Cache.GetAsync<ICollection<FindHit<T>>>(cacheKey).AnyContext();
                result = cacheKeyHits.HasValue && !cacheKeyHits.IsNull
                    ? cacheKeyHits.Value.Where(v => v?.Document != null && idList.Contains(v.Id))
                    : Enumerable.Empty<FindHit<T>>();
            }

            // Note: the distinct by is an extra safety check just in case we ever get into a weird state.
            var distinctResults = result.DistinctBy(v => v.Id).ToList();

            if (_logger.IsEnabled(Microsoft.Extensions.Logging.LogLevel.Trace))
                _logger.LogTrace("Cache {HitOrMiss}: type={EntityType} key={CacheKey}", (distinctResults.Count > 0 ? "hit" : "miss"), EntityTypeName, cacheKey ?? String.Join(", ", idList));

            return distinctResults;
        } catch (Exception ex) {
            _logger.LogError(ex, "Error getting cached find hit: type={EntityType} key={CacheKey}", EntityTypeName, cacheKey ?? String.Join(", ", idList));

            return Enumerable.Empty<FindHit<T>>().ToList();
        }
    }

    protected FindHit<T> ToFindHit(T document) {
        string version = HasVersion ? ((IVersioned)document)?.Version : null;
        string routing = GetParentIdFunc?.Invoke(document);
        var idDocument = document as IIdentity;
        return new FindHit<T>(idDocument?.Id, document, 0, version, routing);
    }

    protected Task AddDocumentsToCacheAsync(T document, ICommandOptions options, bool isDirtyRead) {
        return AddDocumentsToCacheAsync(new[] { document }, options, isDirtyRead);
    }

    protected Task AddDocumentsToCacheAsync(ICollection<T> documents, ICommandOptions options, bool isDirtyRead) {
        return AddDocumentsToCacheAsync(documents.Select(ToFindHit).ToList(), options, isDirtyRead);
    }

    protected Task AddDocumentsToCacheAsync(FindHit<T> findHit, ICommandOptions options, bool isDirtyRead) {
        return AddDocumentsToCacheAsync(new[] { findHit }, options, isDirtyRead);
    }

    protected virtual async Task AddDocumentsToCacheAsync(ICollection<FindHit<T>> findHits, ICommandOptions options, bool isDirtyRead) {
        if (options.HasCacheKey())
            await Cache.SetAsync(options.GetCacheKey(), findHits, options.GetExpiresIn()).AnyContext();

        // don't add dirty read documents by id as they may be out of sync due to eventual consistency
        if (isDirtyRead)
            return;

        var findHitsById = findHits
            .Where(hit => hit?.Id != null)
            .ToDictionary(hit => hit.Id, hit => (ICollection<FindHit<T>>)findHits.Where(h => h.Id == hit.Id).ToList());

        if (findHitsById.Count == 0)
            return;

        await Cache.SetAllAsync(findHitsById, options.GetExpiresIn()).AnyContext();

        if (_logger.IsEnabled(Microsoft.Extensions.Logging.LogLevel.Trace))
            _logger.LogTrace("Add documents to cache: type={EntityType} ids={Ids}", EntityTypeName, String.Join(", ", findHits.Select(h => h?.Id)));
    }

    protected Task AddDocumentsToCacheWithKeyAsync(IDictionary<string, T> documents, TimeSpan expiresIn) {
        return Cache.SetAllAsync(documents.ToDictionary(kvp => kvp.Key, kvp => (ICollection<FindHit<T>>)new List<FindHit<T>> { ToFindHit(kvp.Value) }), expiresIn);
    }

    protected Task AddDocumentsToCacheWithKeyAsync(IDictionary<string, FindHit<T>> findHits, TimeSpan expiresIn) {
        return Cache.SetAllAsync(findHits.ToDictionary(kvp => kvp.Key, kvp => (ICollection<FindHit<T>>)new List<FindHit<T>> { kvp.Value }), expiresIn);
    }

    protected Task AddDocumentsToCacheWithKeyAsync(string cacheKey, T document, TimeSpan expiresIn) {

        return AddDocumentsToCacheWithKeyAsync(cacheKey, ToFindHit(document), expiresIn);
    }

    protected Task AddDocumentsToCacheWithKeyAsync(string cacheKey, FindHit<T> findHit, TimeSpan expiresIn) {
        return Cache.SetAsync<ICollection<FindHit<T>>>(cacheKey, new[] { findHit }, expiresIn);
    }
}

internal class SearchResponse<TDocument> : IResponse, IElasticsearchResponse where TDocument : class {
    public IApiCallDetails ApiCall { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

    public string DebugInformation => throw new NotImplementedException();

    public bool IsValid => throw new NotImplementedException();

    public Exception OriginalException => throw new NotImplementedException();

    public ServerError ServerError => throw new NotImplementedException();

    AggregateDictionary Aggregations { get; }
    bool TimedOut { get; }
    bool TerminatedEarly { get; }
    ISuggestDictionary<TDocument> Suggest { get; }
    ShardStatistics Shards { get; }
    string ScrollId { get; }
    Profile Profile { get; }
    long Took { get; }
    string PointInTimeId { get; }
    double MaxScore { get; }
    IHitsMetadata<TDocument> HitsMetadata { get; }
    IReadOnlyCollection<IHit<TDocument>> Hits { get; }
    IReadOnlyCollection<FieldValues> Fields { get; }
    IReadOnlyCollection<TDocument> Documents { get; }
    ClusterStatistics Clusters { get; }
    long NumberOfReducePhases { get; }
    long Total { get; }

    public bool TryGetServerErrorReason(out string reason) {
        throw new NotImplementedException();
    }
}
