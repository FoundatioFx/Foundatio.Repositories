using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Aggregations;
using Elastic.Clients.Elasticsearch.AsyncSearch;
using Elastic.Clients.Elasticsearch.Core.MGet;
using Elastic.Clients.Elasticsearch.Core.Search;
using Elastic.Clients.Elasticsearch.QueryDsl;
using Elastic.Transport;
using Elastic.Transport.Products.Elasticsearch;
using Foundatio.Caching;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.CustomFields;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Elasticsearch.Utility;
using Foundatio.Repositories.Exceptions;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Options;
using Foundatio.Repositories.Queries;
using Foundatio.Resilience;
using Foundatio.Utility;
using Microsoft.Extensions.Logging;

namespace Foundatio.Repositories.Elasticsearch;

public abstract class ElasticReadOnlyRepositoryBase<T> : ISearchableReadOnlyRepository<T> where T : class, new()
{
    protected static readonly bool HasIdentity = typeof(IIdentity).IsAssignableFrom(typeof(T));
    protected static readonly bool HasDates = typeof(IHaveDates).IsAssignableFrom(typeof(T));
    protected static readonly bool HasCreatedDate = typeof(IHaveCreatedDate).IsAssignableFrom(typeof(T));
    protected static readonly bool SupportsSoftDeletes = typeof(ISupportSoftDeletes).IsAssignableFrom(typeof(T));
    protected static readonly bool HasVersion = typeof(IVersioned).IsAssignableFrom(typeof(T));
    protected static readonly bool HasCustomFields = typeof(IHaveCustomFields).IsAssignableFrom(typeof(T)) || typeof(IHaveVirtualCustomFields).IsAssignableFrom(typeof(T));
    protected static readonly string EntityTypeName = typeof(T).Name;
    protected static readonly IReadOnlyCollection<T> EmptyList = new List<T>(0).AsReadOnly();
    private readonly List<Lazy<Field>> _defaultExcludes = new();
    protected readonly Lazy<string> _idField;

    protected readonly ILogger _logger;
    protected readonly Lazy<ElasticsearchClient> _lazyClient;
    protected ElasticsearchClient _client => _lazyClient.Value;
    protected readonly IResiliencePolicyProvider _resiliencePolicyProvider;
    protected readonly IResiliencePolicy _resiliencePolicy;

    private ScopedCacheClient _scopedCacheClient;

    protected ElasticReadOnlyRepositoryBase(IIndex index)
    {
        ElasticIndex = index;
        if (HasIdentity)
            _idField = new Lazy<string>(() => InferField(d => ((IIdentity)d).Id) ?? "id");
        _lazyClient = new Lazy<ElasticsearchClient>(() => index.Configuration.Client);

        SetCacheClient(index.Configuration.Cache);
        _logger = index.Configuration.LoggerFactory.CreateLogger(GetType());

        _resiliencePolicyProvider = index.Configuration.ResiliencePolicyProvider;
        _resiliencePolicy = _resiliencePolicyProvider.GetPolicy([GetType()], fallback => fallback.WithUnhandledException<DocumentNotFoundException>(), _logger, ElasticIndex.Configuration.TimeProvider);
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

    public Task<T> GetByIdAsync(Id id, CommandOptionsDescriptor<T> options)
    {
        return GetByIdAsync(id, options.Configure());
    }

    public virtual async Task<T> GetByIdAsync(Id id, ICommandOptions options = null)
    {
        if (String.IsNullOrEmpty(id.Value))
            return null;

        options = ConfigureOptions(options.As<T>());

        await OnBeforeGetAsync(new Ids(id), options, typeof(T));

        // we don't have the parent id so we have to do a query
        if (HasParent && id.Routing == null)
        {
            var result = await FindOneAsync(NewQuery().Id(id), options).AnyContext();
            return result?.Document;
        }

        if (IsCacheEnabled && options.ShouldReadCache())
        {
            var value = await GetCachedFindHit(id, options.GetCacheKey()).AnyContext();

            if (value?.Document != null)
            {
                _logger.LogTrace("Cache hit: type={EntityType} key={Id}", EntityTypeName, id);

                return ShouldReturnDocument(value.Document, options) ? value.Document : null;
            }
        }

        var request = new GetRequest(ElasticIndex.GetIndex(id), id.Value);

        if (id.Routing != null)
            request.Routing = id.Routing;

        ConfigureGetRequest(request, options);
        var response = await _client.GetAsync<T>(request).AnyContext();
        _logger.LogRequest(response, options.GetQueryLogLevel());

        var findHit = response.Found ? response.ToFindHit() : null;

        if (IsCacheEnabled && options.ShouldUseCache())
            await AddDocumentsToCacheAsync(findHit ?? new FindHit<T>(id, null, 0), options, false).AnyContext();

        return ShouldReturnDocument(findHit?.Document, options) ? findHit?.Document : null;
    }

    public Task<IReadOnlyCollection<T>> GetByIdsAsync(Ids ids, CommandOptionsDescriptor<T> options)
    {
        return GetByIdsAsync(ids, options.Configure());
    }

    public virtual async Task<IReadOnlyCollection<T>> GetByIdsAsync(Ids ids, ICommandOptions options = null)
    {
        var idList = ids?.Distinct().Where(i => !String.IsNullOrEmpty(i)).ToList();
        if (idList == null || idList.Count == 0)
            return EmptyList;

        if (!HasIdentity)
            throw new NotSupportedException("Model type must implement IIdentity.");

        options = ConfigureOptions(options.As<T>());

        await OnBeforeGetAsync(new Ids(idList), options, typeof(T));

        var hits = new List<FindHit<T>>();
        if (IsCacheEnabled && options.ShouldReadCache())
            hits.AddRange(await GetCachedFindHit(idList, options.GetCacheKey()).AnyContext());

        var itemsToFind = idList.Except(hits.Select(i => (Id)i.Id)).ToList();
        if (itemsToFind.Count == 0)
            return hits.Where(h => h.Document != null && ShouldReturnDocument(h.Document, options)).Select(h => h.Document).ToList().AsReadOnly();

        // Build MultiGetOperation objects for each ID
        var itemsForMultiGet = itemsToFind.Where(i => i.Routing != null || !HasParent).ToList();
        if (itemsForMultiGet.Count > 0)
        {
            var docOperations = itemsForMultiGet
                .Select(id =>
                {
                    var op = new MultiGetOperation(id.Value) { Index = ElasticIndex.GetIndex(id) };
                    if (id.Routing != null)
                        op.Routing = id.Routing;
                    return op;
                })
                .ToList();

            var multiGet = new MultiGetRequestDescriptor().Docs(docOperations);

            ConfigureMultiGetRequest(multiGet, options);
            var multiGetResults = await _client.MultiGetAsync<T>(multiGet).AnyContext();
            _logger.LogRequest(multiGetResults, options.GetQueryLogLevel());

            foreach (var findHit in multiGetResults.ToFindHits())
            {
                hits.Add(findHit);
                itemsToFind.Remove(new Id(findHit.Id, findHit.Routing));
            }
        }

        // fallback to doing a find
        if (itemsToFind.Count > 0 && (HasParent || ElasticIndex.HasMultipleIndexes))
        {
            var response = await FindAsync(q => q.Id(itemsToFind.Select(id => id.Value)), o => o.PageLimit(1000)).AnyContext();
            do
            {
                if (response.Hits.Count > 0)
                {
                    foreach (var hit in response.Hits.Where(h => h.Document != null))
                    {
                        hits.Add(hit);
                        itemsToFind.Remove(new Id(hit.Id, hit.Routing));
                    }
                }
            } while (await response.NextPageAsync().AnyContext());
        }

        if (IsCacheEnabled && options.ShouldUseCache())
        {
            // Add null markers for IDs that were not found (to cache the "not found" result)
            foreach (var id in itemsToFind)
                hits.Add(new FindHit<T>(id, null, 0));

            await AddDocumentsToCacheAsync(hits, options, false).AnyContext();
        }

        return hits.Where(h => h.Document != null && ShouldReturnDocument(h.Document, options)).Select(h => h.Document).ToList().AsReadOnly();
    }

    public Task<FindResults<T>> GetAllAsync(CommandOptionsDescriptor<T> options)
    {
        return GetAllAsync(options.Configure());
    }

    public virtual Task<FindResults<T>> GetAllAsync(ICommandOptions options = null)
    {
        return FindAsync(NewQuery(), options);
    }

    public Task<bool> ExistsAsync(Id id, CommandOptionsDescriptor<T> options)
    {
        return ExistsAsync(id, options.Configure());
    }

    public virtual async Task<bool> ExistsAsync(Id id, ICommandOptions options = null)
    {
        if (String.IsNullOrEmpty(id.Value))
            return false;

        // documents that use soft deletes or have parents without a routing id need to use search for exists
        if (!SupportsSoftDeletes && (!HasParent || id.Routing != null))
        {
            var request = new ExistsRequest(ElasticIndex.GetIndex(id), id.Value);
            if (id.Routing != null)
                request.Routing = id.Routing;

            var response = await _client.ExistsAsync(request).AnyContext();
            _logger.LogRequest(response, options.GetQueryLogLevel());

            return response.Exists;
        }

        return await ExistsAsync(q => q.Id(id), o => options.As<T>()).AnyContext();
    }

    public Task<CountResult> CountAsync(CommandOptionsDescriptor<T> options)
    {
        return CountAsync(options.Configure());
    }

    public virtual async Task<CountResult> CountAsync(ICommandOptions options = null)
    {
        var result = await CountAsync(NewQuery(), options).AnyContext();
        return result;
    }

    public Task InvalidateCacheAsync(T document)
    {
        return InvalidateCacheAsync(new[] { document });
    }

    public Task InvalidateCacheAsync(IEnumerable<T> documents)
    {
        var docs = documents?.ToList();
        if (docs == null || docs.Any(d => d == null))
            throw new ArgumentNullException(nameof(documents));

        return InvalidateCacheAsync(docs.Select(d => new ModifiedDocument<T>(d, null)).ToList());
    }

    public Task InvalidateCacheAsync(string cacheKey)
    {
        return InvalidateCacheAsync(new[] { cacheKey });
    }

    public Task InvalidateCacheAsync(IEnumerable<string> cacheKeys)
    {
        var keys = cacheKeys?.ToList();
        if (keys == null || keys.Any(k => k == null))
            throw new ArgumentNullException(nameof(cacheKeys));

        if (keys.Count > 0)
            return Cache.RemoveAllAsync(keys);

        return Task.CompletedTask;
    }

    public AsyncEvent<BeforeGetEventArgs<T>> BeforeGet { get; } = new AsyncEvent<BeforeGetEventArgs<T>>();

    protected async Task OnBeforeGetAsync(Ids ids, ICommandOptions options, Type resultType)
    {
        if (BeforeGet is not { HasHandlers: true })
            return;

        await BeforeGet.InvokeAsync(this, new BeforeGetEventArgs<T>(ids, options, this, resultType)).AnyContext();
    }

    public AsyncEvent<BeforeQueryEventArgs<T>> BeforeQuery { get; } = new AsyncEvent<BeforeQueryEventArgs<T>>();

    protected async Task OnBeforeQueryAsync(IRepositoryQuery query, ICommandOptions options, Type resultType)
    {
        if (SupportsSoftDeletes && IsCacheEnabled && options.GetSoftDeleteMode() == SoftDeleteQueryMode.ActiveOnly)
        {
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

    public virtual Task<FindResults<T>> FindAsync(RepositoryQueryDescriptor<T> query, CommandOptionsDescriptor<T> options = null)
    {
        return FindAsync(query.Configure(), options.Configure());
    }

    public Task<FindResults<T>> FindAsync(IRepositoryQuery query, ICommandOptions options = null)
    {
        return FindAsAsync<T>(query, options);
    }

    public Task<FindResults<TResult>> FindAsAsync<TResult>(RepositoryQueryDescriptor<T> query, CommandOptionsDescriptor<T> options = null) where TResult : class, new()
    {
        return FindAsAsync<TResult>(query.Configure(), options.Configure());
    }

    public virtual async Task<FindResults<TResult>> FindAsAsync<TResult>(IRepositoryQuery query, ICommandOptions options = null) where TResult : class, new()
    {
        options = ConfigureOptions(options.As<T>());
        bool useSnapshotPaging = options.ShouldUseSnapshotPaging();
        // don't use caching with snapshot paging.
        bool allowCaching = IsCacheEnabled && useSnapshotPaging == false;

        await OnBeforeQueryAsync(query, options, typeof(TResult)).AnyContext();

        await RefreshForConsistency(query, options).AnyContext();

        string cacheSuffix = options?.HasPageLimit() == true ? String.Concat(options.GetPage().ToString(), ":", options.GetLimit().ToString()) : null;

        FindResults<TResult> result;
        if (allowCaching)
        {
            result = await GetCachedQueryResultAsync<FindResults<TResult>>(options, cacheSuffix: cacheSuffix).AnyContext();
            if (result != null)
            {
                ((IFindResults<TResult>)result).GetNextPageFunc = async previousResults => await GetNextPageFunc(previousResults, query, options).AnyContext();
                return result;
            }
        }

        if (options.HasAsyncQueryId())
        {
            var queryId = options.GetAsyncQueryId();
            if (String.IsNullOrEmpty(queryId))
                throw new ArgumentNullException("AsyncQueryId must not be null");

            var response = await _client.AsyncSearch.GetAsync<TResult>(queryId, s =>
            {
                if (options.HasAsyncQueryWaitTime())
                    s.WaitForCompletionTimeout(options.GetAsyncQueryWaitTime());
            }).AnyContext();

            if (options.ShouldAutoDeleteAsyncQuery() && !response.IsRunning)
                await RemoveQueryAsync(queryId);

            _logger.LogRequest(response, options.GetQueryLogLevel());
            if (!response.IsValidResponse && response.ApiCallDetails.HttpStatusCode.GetValueOrDefault() == 404)
                throw new AsyncQueryNotFoundException(queryId);

            result = response.ToFindResults(options);
        }
        else if (options.HasSnapshotScrollId())
        {
            var scrollRequest = new ScrollRequest(options.GetSnapshotScrollId()) { Scroll = options.GetSnapshotLifetime() };
            var response = await _client.ScrollAsync<TResult>(scrollRequest).AnyContext();
            _logger.LogRequest(response, options.GetQueryLogLevel());
            result = response.ToFindResults(options);
        }
        else
        {
            var searchDescriptor = await CreateSearchDescriptorAsync(query, options).AnyContext();
            if (useSnapshotPaging)
                searchDescriptor.Scroll(options.GetSnapshotLifetime());

            if (query.ShouldOnlyHaveIds())
                searchDescriptor.Source(false);

            if (options.ShouldUseAsyncQuery())
            {
                SearchRequest searchRequest = searchDescriptor;
                var asyncSearchRequest = searchRequest.ToAsyncSearchSubmitRequest<TResult>();

                if (options.HasAsyncQueryWaitTime())
                    asyncSearchRequest.WaitForCompletionTimeout = options.GetAsyncQueryWaitTime();

                var response = await _client.AsyncSearch.SubmitAsync<TResult>(asyncSearchRequest).AnyContext();
                _logger.LogRequest(response, options.GetQueryLogLevel());
                result = response.ToFindResults(options);
            }
            else
            {
                var response = await _client.SearchAsync<TResult>(searchDescriptor).AnyContext();
                _logger.LogRequest(response, options.GetQueryLogLevel());
                result = response.ToFindResults(options);
            }
        }

        if (useSnapshotPaging && !result.HasMore)
        {
            // clear the scroll
            string scrollId = result.GetScrollId();
            if (!String.IsNullOrEmpty(scrollId))
            {
                var response = await _client.ClearScrollAsync(s => s.ScrollId(result.GetScrollId())).AnyContext();
                _logger.LogRequest(response, options.GetQueryLogLevel());
            }
        }

        if (allowCaching && !result.IsAsyncQueryRunning() && !result.IsAsyncQueryPartial())
            await SetCachedQueryResultAsync(options, result, cacheSuffix: cacheSuffix).AnyContext();

        ((IFindResults<TResult>)result).GetNextPageFunc = previousResults => GetNextPageFunc(previousResults, query, options);

        return result;
    }

    public async Task RemoveQueryAsync(string queryId)
    {
        var response = await _client.AsyncSearch.DeleteAsync(queryId);
        _logger.LogRequest(response);
    }

    private async Task<FindResults<TResult>> GetNextPageFunc<TResult>(FindResults<TResult> previousResults, IRepositoryQuery query, ICommandOptions options) where TResult : class, new()
    {
        if (previousResults == null)
            throw new ArgumentException(nameof(previousResults));

        string scrollId = previousResults.GetScrollId();
        if (!String.IsNullOrEmpty(scrollId))
        {
            var scrollRequest = new ScrollRequest(scrollId) { Scroll = options.GetSnapshotLifetime() };
            var scrollResponse = await _client.ScrollAsync<TResult>(scrollRequest).AnyContext();
            _logger.LogRequest(scrollResponse, options.GetQueryLogLevel());

            var results = scrollResponse.ToFindResults(options);
            ((IFindResults<T>)results).Page = previousResults.Page + 1;

            // clear the scroll
            if (!results.HasMore)
            {
                var clearScrollResponse = await _client.ClearScrollAsync(s => s.ScrollId(scrollId));
                _logger.LogRequest(clearScrollResponse, options.GetQueryLogLevel());
            }

            return results;
        }

        if (options.ShouldUseSearchAfterPaging())
            options.SearchAfterToken(previousResults.GetSearchAfterToken());

        if (options == null)
            return new FindResults<TResult>();

        options?.PageNumber(!options.HasPageNumber() ? 2 : options.GetPage() + 1);
        return await FindAsAsync<TResult>(query, options).AnyContext();
    }

    public Task<FindHit<T>> FindOneAsync(RepositoryQueryDescriptor<T> query, CommandOptionsDescriptor<T> options = null)
    {
        return FindOneAsync(query.Configure(), options.Configure());
    }

    public virtual async Task<FindHit<T>> FindOneAsync(IRepositoryQuery query, ICommandOptions options = null)
    {
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
        _logger.LogRequest(response, options.GetQueryLogLevel());

        if (!response.IsValidResponse)
        {
            if (response.ApiCallDetails.HttpStatusCode.GetValueOrDefault() == 404)
                return FindHit<T>.Empty;

            throw new DocumentException(response.GetErrorMessage("Error while finding document"), response.OriginalException());
        }

        result = response.Hits.Select(h => h.ToFindHit()).ToList();

        if (IsCacheEnabled && options.ShouldUseCache())
            await AddDocumentsToCacheAsync(result, options, options.GetConsistency(DefaultConsistency) == Consistency.Eventual).AnyContext();

        return result.FirstOrDefault();
    }

    public Task<CountResult> CountAsync(RepositoryQueryDescriptor<T> query, CommandOptionsDescriptor<T> options = null)
    {
        return CountAsync(query.Configure(), options.Configure());
    }

    public virtual async Task<CountResult> CountAsync(IRepositoryQuery query, ICommandOptions options = null)
    {
        options = ConfigureOptions(options.As<T>());

        CountResult result;
        if (IsCacheEnabled && options.ShouldReadCache())
        {
            result = await GetCachedQueryResultAsync<CountResult>(options, "count").AnyContext();
            if (result != null)
                return result;
        }

        await OnBeforeQueryAsync(query, options, typeof(T)).AnyContext();

        await RefreshForConsistency(query, options).AnyContext();

        var searchDescriptor = await CreateSearchDescriptorAsync(query, options).AnyContext();
        searchDescriptor.Size(0);

        if (options.HasAsyncQueryId())
        {
            var queryId = options.GetAsyncQueryId();
            if (String.IsNullOrEmpty(queryId))
                throw new ArgumentNullException("AsyncQueryId must not be null");

            var response = await _client.AsyncSearch.GetAsync<T>(queryId, s =>
            {
                if (options.HasAsyncQueryWaitTime())
                    s.WaitForCompletionTimeout(options.GetAsyncQueryWaitTime());
            }).AnyContext();
            _logger.LogRequest(response, options.GetQueryLogLevel());

            if (options.ShouldAutoDeleteAsyncQuery() && !response.IsRunning)
                await RemoveQueryAsync(queryId);

            result = response.ToCountResult(options);
        }
        else if (options.ShouldUseAsyncQuery())
        {
            var response = await _client.AsyncSearch.SubmitAsync<T>(s =>
            {
                string[] indices = ElasticIndex.GetIndexesByQuery(query);
                if (indices?.Length > 0)
                    s.Indices(String.Join(",", indices));
                s.Size(0);

                if (options.HasAsyncQueryWaitTime())
                    s.WaitForCompletionTimeout(options.GetAsyncQueryWaitTime());
            }).AnyContext();
            _logger.LogRequest(response, options.GetQueryLogLevel());
            result = response.ToCountResult(options);
        }
        else
        {
            var response = await _client.SearchAsync<T>(searchDescriptor).AnyContext();
            _logger.LogRequest(response, options.GetQueryLogLevel());
            result = response.ToCountResult(options);
        }

        if (IsCacheEnabled && options.ShouldUseCache() && !result.IsAsyncQueryRunning() && !result.IsAsyncQueryPartial())
            await SetCachedQueryResultAsync(options, result, "count").AnyContext();

        return result;
    }

    public Task<bool> ExistsAsync(RepositoryQueryDescriptor<T> query, CommandOptionsDescriptor<T> options = null)
    {
        return ExistsAsync(query.Configure(), options.Configure());
    }

    public virtual async Task<bool> ExistsAsync(IRepositoryQuery query, ICommandOptions options = null)
    {
        options = ConfigureOptions(options.As<T>());
        await OnBeforeQueryAsync(query, options, typeof(T)).AnyContext();

        await RefreshForConsistency(query, options).AnyContext();

        var searchDescriptor = (await CreateSearchDescriptorAsync(query, options).AnyContext()).Size(0);
        searchDescriptor.DocvalueFields(new FieldAndFormat[] { new() { Field = _idField.Value } });
        var response = await _client.SearchAsync<T>(searchDescriptor).AnyContext();
        _logger.LogRequest(response, options.GetQueryLogLevel());

        if (!response.IsValidResponse)
        {
            if (response.ApiCallDetails.HttpStatusCode.GetValueOrDefault() == 404)
                return false;

            throw new DocumentException(response.GetErrorMessage("Error checking if document exists"), response.OriginalException());
        }

        return response.Total > 0;
    }

    public virtual Task<FindResults<T>> SearchAsync(ISystemFilter systemFilter, string filter = null, string criteria = null, string sort = null, string aggregations = null, ICommandOptions options = null)
    {
        return FindAsAsync<T>(q => q.SystemFilter(systemFilter).FilterExpression(filter).SearchExpression(criteria).SortExpression(sort).AggregationsExpression(aggregations), o => options.As<T>());
    }

    public virtual Task<CountResult> CountBySearchAsync(ISystemFilter systemFilter, string filter = null, string aggregations = null, ICommandOptions options = null)
    {
        return CountAsync(q => q.SystemFilter(systemFilter).FilterExpression(filter).AggregationsExpression(aggregations), o => options.As<T>());
    }

    #endregion

    protected virtual IRepositoryQuery<T> NewQuery()
    {
        return new RepositoryQuery<T>();
    }

    protected virtual IRepositoryQuery ConfigureQuery(IRepositoryQuery<T> query)
    {
        if (query == null)
            query = new RepositoryQuery<T>();

        if (_defaultExcludes.Count > 0 && query.GetExcludes().Count == 0)
            query.Exclude(_defaultExcludes.Select(e => e.Value));

        return query;
    }

    protected virtual Task InvalidateCacheByQueryAsync(IRepositoryQuery<T> query)
    {
        return InvalidateCacheAsync(query.GetIds());
    }

    /// <summary>
    /// Registers a field to be excluded by default from Elasticsearch <c>_source</c> filtering.
    /// Default excludes are applied only when no explicit excludes are set on the query;
    /// setting any explicit exclude on a query will cause all default excludes to be skipped.
    /// </summary>
    protected void AddDefaultExclude(string field)
    {
        _defaultExcludes.Add(new Lazy<Field>(() => field));
    }

    /// <inheritdoc cref="AddDefaultExclude(string)"/>
    protected void AddDefaultExclude(Lazy<string> field)
    {
        _defaultExcludes.Add(new Lazy<Field>(() => field.Value));
    }

    /// <inheritdoc cref="AddDefaultExclude(string)"/>
    protected void AddDefaultExclude(Expression<Func<T, object>> objectPath)
    {
        _defaultExcludes.Add(new Lazy<Field>(() => InferPropertyName(objectPath)));
    }

    /// <inheritdoc cref="AddDefaultExclude(string)"/>
    protected void AddDefaultExclude(params Expression<Func<T, object>>[] objectPaths)
    {
        _defaultExcludes.AddRange(objectPaths.Select(o => new Lazy<Field>(() => InferPropertyName(o))));
    }

    public bool IsCacheEnabled { get; private set; } = false;
    protected ScopedCacheClient Cache => _scopedCacheClient ?? new ScopedCacheClient(new NullCacheClient(), null);

    private void SetCacheClient(ICacheClient cache)
    {
        IsCacheEnabled = cache != null && cache is not NullCacheClient;
        _scopedCacheClient = new ScopedCacheClient(cache ?? new NullCacheClient(), EntityTypeName);
    }

    protected void DisableCache()
    {
        IsCacheEnabled = false;
        _scopedCacheClient = new ScopedCacheClient(new NullCacheClient(), EntityTypeName);
    }

    protected virtual Task InvalidateCacheAsync(IReadOnlyCollection<ModifiedDocument<T>> documents, Foundatio.Repositories.Models.ChangeType? changeType = null)
    {
        var keysToRemove = new HashSet<string>();

        if (IsCacheEnabled && HasIdentity && changeType != Foundatio.Repositories.Models.ChangeType.Added)
        {
            foreach (var document in documents)
            {
                keysToRemove.Add(((IIdentity)document.Value).Id);
                if (((IIdentity)document.Original)?.Id != null)
                    keysToRemove.Add(((IIdentity)document.Original).Id);
            }
        }

        if (keysToRemove.Count > 0)
            return Cache.RemoveAllAsync(keysToRemove);

        return Task.CompletedTask;
    }

    protected virtual Task<SearchRequestDescriptor<T>> CreateSearchDescriptorAsync(IRepositoryQuery query, ICommandOptions options)
    {
        return ConfigureSearchDescriptorAsync(new SearchRequestDescriptor<T>(), query, options);
    }

    protected virtual async Task<SearchRequestDescriptor<T>> ConfigureSearchDescriptorAsync(SearchRequestDescriptor<T> search, IRepositoryQuery query, ICommandOptions options)
    {

        query = ConfigureQuery(query.As<T>()).Unwrap();
        string[] indices = ElasticIndex.GetIndexesByQuery(query);
        if (indices?.Length > 0)
            search.Indices(String.Join(",", indices));
        if (HasVersion)
            search.SeqNoPrimaryTerm(HasVersion);

        if (options.HasQueryTimeout())
            search.Timeout(options.GetQueryTimeout().ToString());

        search.IgnoreUnavailable();
        search.TrackTotalHits(new TrackHits(true));

        await ElasticIndex.QueryBuilder.ConfigureSearchAsync(query, options, search).AnyContext();

        return search;
    }

    protected virtual ICommandOptions<T> ConfigureOptions(ICommandOptions<T> options)
    {
        options ??= new CommandOptions<T>();

        options.TimeProvider(ElasticIndex.Configuration.TimeProvider ?? TimeProvider.System);
        options.ElasticIndex(ElasticIndex);
        options.SupportsSoftDeletes(SupportsSoftDeletes);
        options.DocumentType(typeof(T));
        options.DefaultCacheExpiresIn(DefaultCacheExpiration);
        options.DefaultPageLimit(DefaultPageLimit);
        options.MaxPageLimit(MaxPageLimit);
        options.DefaultQueryLogLevel(DefaultQueryLogLevel);

        return options;
    }

    protected virtual void ConfigureGetRequest(GetRequest request, ICommandOptions options)
    {
        var (resolvedIncludes, resolvedExcludes) = GetResolvedIncludesAndExcludes(options);

        if (resolvedIncludes.Length > 0 && resolvedExcludes.Length > 0)
        {
            request.SourceIncludes = resolvedIncludes;
            request.SourceExcludes = resolvedExcludes;
        }
        else if (resolvedIncludes.Length > 0)
        {
            request.SourceIncludes = resolvedIncludes;
        }
        else if (resolvedExcludes.Length > 0)
        {
            request.SourceExcludes = resolvedExcludes;
        }
    }

    protected virtual void ConfigureMultiGetRequest(MultiGetRequestDescriptor request, ICommandOptions options)
    {
        var (resolvedIncludes, resolvedExcludes) = GetResolvedIncludesAndExcludes(options);

        if (resolvedIncludes.Length > 0 && resolvedExcludes.Length > 0)
        {
            request.SourceIncludes(resolvedIncludes);
            request.SourceExcludes(resolvedExcludes);
        }
        else if (resolvedIncludes.Length > 0)
        {
            request.SourceIncludes(resolvedIncludes);
        }
        else if (resolvedExcludes.Length > 0)
        {
            request.SourceExcludes(resolvedExcludes);
        }
    }

    protected (Field[] Includes, Field[] Excludes) GetResolvedIncludesAndExcludes(ICommandOptions options)
    {
        return GetResolvedIncludesAndExcludes(null, options);
    }

    protected (Field[] Includes, Field[] Excludes) GetResolvedIncludesAndExcludes(IRepositoryQuery query, ICommandOptions options)
    {
        var includes = new HashSet<Field>();
        includes.AddRange(query.GetIncludes());
        includes.AddRange(options.GetIncludes());
        if (HasIdentity && includes.Count > 0)
            includes.Add(_idField.Value);

        string optionIncludeMask = options.GetIncludeMask();
        if (!String.IsNullOrEmpty(optionIncludeMask))
            includes.AddRange(FieldIncludeParser.ParseFieldPaths(optionIncludeMask).Select(f => (Field)f));

        var resolvedIncludes = ElasticIndex.MappingResolver.GetResolvedFields(includes).ToArray();

        var excludes = new HashSet<Field>();
        excludes.AddRange(query.GetExcludes());
        excludes.AddRange(options.GetExcludes());

        if (_defaultExcludes.Count > 0 && excludes.Count == 0)
            excludes.AddRange(_defaultExcludes.Select(f => f.Value));

        string optionExcludeMask = options.GetExcludeMask();
        if (!String.IsNullOrEmpty(optionExcludeMask))
            excludes.AddRange(FieldIncludeParser.ParseFieldPaths(optionExcludeMask).Select(f => (Field)f));

        // Remove any included fields from excludes
        var resolvedExcludes = ElasticIndex.MappingResolver.GetResolvedFields(excludes)
            .Where(f => !resolvedIncludes.Contains(f))
            .ToArray();

        return (resolvedIncludes, resolvedExcludes);
    }

    protected bool ShouldReturnDocument(T document, ICommandOptions options)
    {
        if (document == null)
            return true;

        if (!SupportsSoftDeletes)
            return true;

        var mode = options.GetSoftDeleteMode();
        bool returnSoftDeletes = mode is SoftDeleteQueryMode.All or SoftDeleteQueryMode.DeletedOnly;
        return returnSoftDeletes || !((ISupportSoftDeletes)document).IsDeleted;
    }

    protected async Task RefreshForConsistency(IRepositoryQuery query, ICommandOptions options)
    {
        // if not using eventual consistency, force a refresh
        if (options.GetConsistency(DefaultConsistency) != Consistency.Eventual)
        {
            string[] indices = ElasticIndex.GetIndexesByQuery(query);
            var response = await _client.Indices.RefreshAsync(indices);
            _logger.LogRequest(response);
        }
    }

    protected async Task<TResult> GetCachedQueryResultAsync<TResult>(ICommandOptions options, string cachePrefix = null, string cacheSuffix = null)
    {
        if (IsCacheEnabled && (options.ShouldUseCache() || options.ShouldReadCache()) && !options.HasCacheKey())
            throw new ArgumentException("Cache key is required when enabling cache.", nameof(options));

        if (!IsCacheEnabled || !options.ShouldReadCache() || !options.HasCacheKey())
            return default;

        string cacheKey = cachePrefix != null ? cachePrefix + ":" + options.GetCacheKey() : options.GetCacheKey();
        if (!String.IsNullOrEmpty(cacheSuffix))
            cacheKey += ":" + cacheSuffix;

        var result = await Cache.GetAsync<TResult>(cacheKey, default).AnyContext();
        _logger.LogTrace("Cache {HitOrMiss}: type={EntityType} key={CacheKey}", (result != null ? "hit" : "miss"), EntityTypeName, cacheKey);

        return result;
    }

    protected async Task SetCachedQueryResultAsync<TResult>(ICommandOptions options, TResult result, string cachePrefix = null, string cacheSuffix = null)
    {
        if (!IsCacheEnabled || result == null || !options.ShouldUseCache())
            return;

        if (!options.HasCacheKey())
            throw new ArgumentException("Cache key is required when enabling cache.", nameof(options));

        string cacheKey = cachePrefix != null ? cachePrefix + ":" + options.GetCacheKey() : options.GetCacheKey();
        if (!String.IsNullOrEmpty(cacheSuffix))
            cacheKey += ":" + cacheSuffix;

        await Cache.SetAsync(cacheKey, result, options.GetExpiresIn()).AnyContext();
        _logger.LogTrace("Set cache: type={EntityType} key={CacheKey}", EntityTypeName, cacheKey);
    }

    protected async Task<ICollection<FindHit<T>>> GetCachedFindHit(ICommandOptions options)
    {
        string cacheKey = options.GetCacheKey();
        try
        {
            var cacheKeyHits = await Cache.GetAsync<ICollection<FindHit<T>>>(cacheKey).AnyContext();

            var result = cacheKeyHits.HasValue && !cacheKeyHits.IsNull ? cacheKeyHits.Value : null;

            _logger.LogTrace("Cache {HitOrMiss}: type={EntityType} key={CacheKey}", (result != null ? "hit" : "miss"), EntityTypeName, cacheKey);

            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting cached find hit: type={EntityType} key={CacheKey}", EntityTypeName, cacheKey);

            return null;
        }
    }

    protected async Task<FindHit<T>> GetCachedFindHit(Id id, string cacheKey = null)
    {
        try
        {
            var cacheKeyHits = await Cache.GetAsync<ICollection<FindHit<T>>>(cacheKey ?? id).AnyContext();

            var result = cacheKeyHits.HasValue && !cacheKeyHits.IsNull
                ? cacheKeyHits.Value.FirstOrDefault(v => v?.Document != null && String.Equals(v.Id, id))
                : null;

            _logger.LogTrace("Cache {HitOrMiss}: type={EntityType} key={CacheKey}", (result != null ? "hit" : "miss"), EntityTypeName, cacheKey ?? id);
            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting cached find hit: type={EntityType} key={CacheKey}", EntityTypeName, cacheKey ?? id);

            return null;
        }
    }

    protected async Task<ICollection<FindHit<T>>> GetCachedFindHit(ICollection<Id> ids, string cacheKey = null)
    {
        var idList = ids.Select(id => id.Value).ToList();
        IEnumerable<FindHit<T>> result;

        try
        {
            if (String.IsNullOrEmpty(cacheKey))
            {
                var cacheHitsById = await Cache.GetAllAsync<ICollection<FindHit<T>>>(idList).AnyContext();
                result = cacheHitsById
                    .Where(kvp => kvp.Value.HasValue && !kvp.Value.IsNull)
                    .SelectMany(kvp => kvp.Value.Value)
                    .Where(v => v?.Document != null && idList.Contains(v.Id));
            }
            else
            {
                var cacheKeyHits = await Cache.GetAsync<ICollection<FindHit<T>>>(cacheKey).AnyContext();
                result = cacheKeyHits.HasValue && !cacheKeyHits.IsNull
                    ? cacheKeyHits.Value.Where(v => v?.Document != null && idList.Contains(v.Id))
                    : Enumerable.Empty<FindHit<T>>();
            }

            // Note: the distinct by is an extra safety check just in case we ever get into a weird state.
            var distinctResults = result.DistinctBy(v => v.Id).ToList();

            _logger.LogTrace("Cache {HitOrMiss}: type={EntityType} key={CacheKey}", (distinctResults.Count > 0 ? "hit" : "miss"), EntityTypeName, cacheKey ?? String.Join(", ", idList));

            return distinctResults;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting cached find hit: type={EntityType} key={CacheKey}", EntityTypeName, cacheKey ?? String.Join(", ", idList));

            return Enumerable.Empty<FindHit<T>>().ToList();
        }
    }

    protected FindHit<T> ToFindHit(T document)
    {
        string version = HasVersion ? ((IVersioned)document)?.Version : null;
        string routing = GetParentIdFunc?.Invoke(document);
        var idDocument = document as IIdentity;
        return new FindHit<T>(idDocument?.Id, document, 0, version, routing);
    }

    protected Task AddDocumentsToCacheAsync(T document, ICommandOptions options, bool isDirtyRead)
    {
        return AddDocumentsToCacheAsync(new[] { document }, options, isDirtyRead);
    }

    protected Task AddDocumentsToCacheAsync(ICollection<T> documents, ICommandOptions options, bool isDirtyRead)
    {
        return AddDocumentsToCacheAsync(documents.Select(ToFindHit).ToList(), options, isDirtyRead);
    }

    protected Task AddDocumentsToCacheAsync(FindHit<T> findHit, ICommandOptions options, bool isDirtyRead)
    {
        return AddDocumentsToCacheAsync(new[] { findHit }, options, isDirtyRead);
    }

    protected virtual async Task AddDocumentsToCacheAsync(ICollection<FindHit<T>> findHits, ICommandOptions options, bool isDirtyRead)
    {
        if (options.HasCacheKey())
        {
            await Cache.SetAsync(options.GetCacheKey(), findHits, options.GetExpiresIn()).AnyContext();

            // NOTE: Custom cache keys store the complete filtered result, but ID-based caching is skipped when includes/excludes are present to avoid incomplete data.
            // This method also doesn't take into account any query includes or excludes but GetById(s) requests don't specify a query.
            if (options.GetIncludes().Count > 0 || options.GetExcludes().Count > 0)
                return;
        }

        // don't add dirty read documents by id as they may be out of sync due to eventual consistency
        if (isDirtyRead)
            return;

        var findHitsById = findHits
            .Where(hit => hit?.Id != null)
            .ToDictionary(hit => hit.Id, hit => (ICollection<FindHit<T>>)findHits.Where(h => h.Id == hit.Id).ToList());

        if (findHitsById.Count == 0)
            return;

        await Cache.SetAllAsync(findHitsById, options.GetExpiresIn()).AnyContext();

        _logger.LogTrace("Add documents to cache: type={EntityType} ids={Ids}", EntityTypeName, String.Join(", ", findHits.Select(h => h?.Id)));
    }

    protected Task AddDocumentsToCacheWithKeyAsync(IDictionary<string, T> documents, TimeSpan expiresIn)
    {
        return Cache.SetAllAsync(documents.ToDictionary(kvp => kvp.Key, kvp => (ICollection<FindHit<T>>)new List<FindHit<T>> { ToFindHit(kvp.Value) }), expiresIn);
    }

    protected Task AddDocumentsToCacheWithKeyAsync(IDictionary<string, FindHit<T>> findHits, TimeSpan expiresIn)
    {
        return Cache.SetAllAsync(findHits.ToDictionary(kvp => kvp.Key, kvp => (ICollection<FindHit<T>>)new List<FindHit<T>> { kvp.Value }), expiresIn);
    }

    protected Task AddDocumentsToCacheWithKeyAsync(string cacheKey, T document, TimeSpan expiresIn)
    {
        return AddDocumentsToCacheWithKeyAsync(cacheKey, ToFindHit(document), expiresIn);
    }

    protected Task AddDocumentsToCacheWithKeyAsync(string cacheKey, FindHit<T> findHit, TimeSpan expiresIn)
    {
        return Cache.SetAsync<ICollection<FindHit<T>>>(cacheKey, new[] { findHit }, expiresIn);
    }
}
