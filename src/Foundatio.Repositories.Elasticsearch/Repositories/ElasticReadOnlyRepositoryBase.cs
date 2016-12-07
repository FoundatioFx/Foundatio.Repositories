using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Foundatio.Caching;
using Foundatio.Repositories.Elasticsearch.Queries;
using Foundatio.Logging;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Models;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Elasticsearch.Queries.Options;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Queries;
using Foundatio.Repositories.Utility;
using Foundatio.Utility;
using Nest;

namespace Foundatio.Repositories.Elasticsearch {
    public abstract class ElasticReadOnlyRepositoryBase<T> : ISearchableReadOnlyRepository<T> where T : class, new() {
        protected static readonly bool HasIdentity = typeof(IIdentity).IsAssignableFrom(typeof(T));
        protected static readonly bool HasDates = typeof(IHaveDates).IsAssignableFrom(typeof(T));
        protected static readonly bool HasCreatedDate = typeof(IHaveCreatedDate).IsAssignableFrom(typeof(T));
        protected static readonly bool SupportsSoftDeletes = typeof(ISupportSoftDeletes).IsAssignableFrom(typeof(T));
        protected static readonly bool HasVersion = typeof(IVersioned).IsAssignableFrom(typeof(T));
        protected static readonly string EntityTypeName = typeof(T).Name;
        protected static readonly IReadOnlyCollection<T> EmptyList = new List<T>(0).AsReadOnly();

        protected readonly ILogger _logger;
        protected readonly IElasticClient _client;

        private ScopedCacheClient _scopedCacheClient;

        protected ElasticReadOnlyRepositoryBase(IIndexType<T> indexType) {
            ElasticType = indexType;
            _client = indexType.Configuration.Client;
            SetCache(indexType.Configuration.Cache);
            _logger = indexType.Configuration.LoggerFactory.CreateLogger(GetType());
        }

        protected Task<FindResults<T>> FindAsync(IRepositoryQuery query) {
            return FindAsAsync<T>(query);
        }

        protected ISet<string> DefaultExcludes { get; } = new HashSet<string>();

        protected async Task<FindResults<TResult>> FindAsAsync<TResult>(IRepositoryQuery query) where TResult : class, new() {
            if (query == null)
                throw new ArgumentNullException(nameof(query));

            var pagableQuery = query as IPagableQuery;
            var pagingOptions = pagableQuery?.Options as IPagingOptions;
            var elasticPagingOptions = pagableQuery?.Options as ElasticPagingOptions;
            bool useSnapshotPaging = elasticPagingOptions?.UseSnapshotPaging ?? false;

            // don't use caching with snapshot paging.
            bool allowCaching = IsCacheEnabled && useSnapshotPaging == false;

            var queryOptions = GetQueryOptions();
            await OnBeforeQueryAsync(query, queryOptions, typeof(TResult)).AnyContext();

            Func<FindResults<TResult>, Task<FindResults<TResult>>> getNextPageFunc = async r => {
                var previousResults = r;
                if (previousResults == null)
                    throw new ArgumentException(nameof(r));

                if (!String.IsNullOrEmpty(previousResults.GetScrollId())) {
                    var scrollResponse = await _client.ScrollAsync<TResult>(pagableQuery.GetLifetime(), previousResults.GetScrollId()).AnyContext();
                    _logger.Trace(() => scrollResponse.GetRequest());

                    var results = scrollResponse.ToFindResults();
                    results.Page = previousResults.Page + 1;
                    results.HasMore = scrollResponse.Hits.Count() >= pagableQuery.GetLimit();
                    return results;
                }

                if (pagableQuery == null)
                    return new FindResults<TResult>();

                if (pagingOptions != null)
                    pagingOptions.Page = pagingOptions.Page == null ? 2 : pagingOptions.Page + 1;

                return await FindAsAsync<TResult>(query).AnyContext();
            };

            string cacheSuffix = pagableQuery?.ShouldUseLimit() == true ? String.Concat(pagingOptions.Page?.ToString() ?? "1", ":", pagableQuery.GetLimit().ToString()) : String.Empty;

            FindResults<TResult> result;
            if (allowCaching) {
                result = await GetCachedQueryResultAsync<FindResults<TResult>>(query, cacheSuffix: cacheSuffix).AnyContext();
                if (result != null) {
                    ((IGetNextPage<TResult>)result).GetNextPageFunc = async r => await getNextPageFunc(r).AnyContext();
                    return result;
                }
            }

            ISearchResponse<TResult> response = null;

            if (useSnapshotPaging == false || String.IsNullOrEmpty(elasticPagingOptions?.ScrollId)) {
                SearchDescriptor<T> searchDescriptor = await CreateSearchDescriptorAsync(query, queryOptions).AnyContext();
                if (useSnapshotPaging)
                    searchDescriptor.Scroll(pagableQuery.GetLifetime());

                response = await _client.SearchAsync<TResult>(searchDescriptor).AnyContext();
            } else {
                response = await _client.ScrollAsync<TResult>(pagableQuery.GetLifetime(), elasticPagingOptions.ScrollId).AnyContext();
            }

            _logger.Trace(() => response.GetRequest());
            if (!response.IsValid) {
                if (response.ApiCall.HttpStatusCode.GetValueOrDefault() == 404)
                    return new FindResults<TResult>();

                string message = response.GetErrorMessage();
                _logger.Error().Exception(response.OriginalException).Message(message).Property("request", response.GetRequest()).Write();
                throw new ApplicationException(message, response.OriginalException);
            }

            if (useSnapshotPaging) {
                result = response.ToFindResults();
                // TODO: Is there a better way to figure out if you are done scrolling?
                result.HasMore = response.Hits.Count() >= pagableQuery.GetLimit();
                ((IGetNextPage<TResult>)result).GetNextPageFunc = getNextPageFunc;
            } else if (pagableQuery?.ShouldUseLimit() == true) {
                result = response.ToFindResults(pagableQuery.GetLimit());
                result.HasMore = response.Hits.Count() > pagableQuery.GetLimit();
                ((IGetNextPage<TResult>)result).GetNextPageFunc = getNextPageFunc;
            } else {
                result = response.ToFindResults();
            }

            result.Page = pagingOptions?.Page ?? 1;

            if (!allowCaching)
                return result;

            var nextPageFunc = ((IGetNextPage<TResult>)result).GetNextPageFunc;
            ((IGetNextPage<TResult>)result).GetNextPageFunc = null;
            await SetCachedQueryResultAsync(query, result, cacheSuffix: cacheSuffix).AnyContext();
            ((IGetNextPage<TResult>)result).GetNextPageFunc = nextPageFunc;

            return result;
        }

        protected async Task<FindHit<T>> FindOneAsync(IRepositoryQuery query) {
            if (query == null)
                throw new ArgumentNullException(nameof(query));

            var result = IsCacheEnabled ? await GetCachedQueryResultAsync<FindHit<T>>(query).AnyContext() : null;
            if (result != null)
                return result;

            var queryOptions = GetQueryOptions();
            await OnBeforeQueryAsync(query, queryOptions, typeof(T)).AnyContext();

            var searchDescriptor = (await CreateSearchDescriptorAsync(query, queryOptions).AnyContext()).Size(1);
            var response = await _client.SearchAsync<T>(searchDescriptor).AnyContext();
            _logger.Trace(() => response.GetRequest());

            if (!response.IsValid) {
                if (response.ApiCall.HttpStatusCode.GetValueOrDefault() == 404)
                    return FindHit<T>.Empty;

                string message = response.GetErrorMessage();
                _logger.Error().Exception(response.OriginalException).Message(message).Property("request", response.GetRequest()).Write();
                throw new ApplicationException(message, response.OriginalException);
            }

            result = response.Hits.FirstOrDefault()?.ToFindHit();
            if (IsCacheEnabled)
                await SetCachedQueryResultAsync(query, result).AnyContext();

            return result;
        }

        public Task<FindResults<T>> SearchAsync(IRepositoryQuery systemFilter, string filter = null, string criteria = null, string sort = null, string aggregations = null, PagingOptions paging = null) {
            var search = NewQuery()
                .WithSystemFilter(systemFilter)
                .WithFilter(filter)
                .WithSearchQuery(criteria, false)
                .WithAggregations(aggregations)
                .WithSort(sort)
                .WithPaging(paging);

            return FindAsync(search);
        }

        public async Task<T> GetByIdAsync(string id, bool useCache = false, TimeSpan? expiresIn = null) {
            if (String.IsNullOrEmpty(id))
                return null;

            T hit = null;
            if (IsCacheEnabled && useCache)
                hit = await Cache.GetAsync<T>(id, default(T)).AnyContext();

            if (hit != null) {
                _logger.Trace(() => $"Cache hit: type={ElasticType.Name} key={id}");
                return hit;
            }

            if (!HasParent) {
                var request = new GetRequest(GetIndexById(id), ElasticType.Name, id);
                var response = await _client.GetAsync<T>(request).AnyContext();
                _logger.Trace(() => response.GetRequest());

                hit = response.Found ? response.ToFindHit().Document : null;
            } else {
                // we don't have the parent id so we have to do a query
                // TODO: Ensure this is find one query is not cached.
                var findResult = await FindOneAsync(NewQuery().WithId(id)).AnyContext();
                if (findResult != null)
                    hit = findResult.Document;
            }

            if (IsCacheEnabled && hit != null && useCache)
                await Cache.SetAsync(id, hit, expiresIn ?? TimeSpan.FromSeconds(ElasticType.DefaultCacheExpirationSeconds)).AnyContext();

            return hit;
        }

        public async Task<IReadOnlyCollection<T>> GetByIdsAsync(IEnumerable<string> ids, bool useCache = false, TimeSpan? expiresIn = null) {
            var idList = ids?.Distinct().Where(i => !String.IsNullOrEmpty(i)).ToList();
            if (idList == null || idList.Count == 0)
                return EmptyList;

            if (!HasIdentity)
                throw new NotSupportedException("Model type must implement IIdentity.");

            var hits = new List<T>();
            if (IsCacheEnabled && useCache) {
                var cacheHits = await Cache.GetAllAsync<T>(idList).AnyContext();
                hits.AddRange(cacheHits.Where(kvp => kvp.Value.HasValue).Select(kvp => kvp.Value.Value));
            }

            var itemsToFind = idList.Except(hits.OfType<IIdentity>().Select(i => i.Id)).ToList();
            if (itemsToFind.Count == 0)
                return hits.AsReadOnly();

            var multiGet = new MultiGetDescriptor();
            if (!HasParent) {
                itemsToFind.ForEach(id => multiGet.Get<T>(f => f.Id(id).Index(GetIndexById(id)).Type(ElasticType.Name)));

                var multiGetResults = await _client.MultiGetAsync(multiGet).AnyContext();
                _logger.Trace(() => multiGetResults.GetRequest());

                foreach (var doc in multiGetResults.Documents) {
                    if (!doc.Found)
                        continue;

                    hits.Add(((IMultiGetHit<T>)doc).ToFindHit().Document);
                    itemsToFind.Remove(doc.Id);
                }
            }

            // fallback to doing a find
            if (itemsToFind.Count > 0 && (HasParent || HasMultipleIndexes))
                hits.AddRange((await FindAsync(NewQuery().WithIds(itemsToFind)).AnyContext()).Hits.Where(h => h.Document != null).Select(h => h.Document));

            if (IsCacheEnabled && useCache) {
                foreach (var item in hits.OfType<IIdentity>())
                    await Cache.SetAsync(item.Id, item, expiresIn.HasValue ? SystemClock.UtcNow.Add(expiresIn.Value) : SystemClock.UtcNow.AddSeconds(ElasticType.DefaultCacheExpirationSeconds)).AnyContext();
            }

            return hits.AsReadOnly();
        }

        public Task<FindResults<T>> GetAllAsync(PagingOptions paging = null) {
            var search = NewQuery()
                .WithPaging(paging);

            return FindAsync(search);
        }

        public async Task<bool> ExistsAsync(string id) {
            if (String.IsNullOrEmpty(id))
                return false;

            return await ExistsAsync(new Query().WithId(id)).AnyContext();
        }

        protected async Task<bool> ExistsAsync(IRepositoryQuery query) {
            if (query == null)
                throw new ArgumentNullException(nameof(query));

            var queryOptions = GetQueryOptions();
            await OnBeforeQueryAsync(query, queryOptions, typeof(T)).AnyContext();

            var searchDescriptor = (await CreateSearchDescriptorAsync(query, queryOptions).AnyContext()).Size(1);
            searchDescriptor.DocvalueFields("id");
            var response = await _client.SearchAsync<T>(searchDescriptor).AnyContext();
            _logger.Trace(() => response.GetRequest());

            if (!response.IsValid) {
                if (response.ApiCall.HttpStatusCode.GetValueOrDefault() == 404)
                    return false;

                string message = response.GetErrorMessage();
                _logger.Error().Exception(response.OriginalException).Message(message).Property("request", response.GetRequest()).Write();
                throw new ApplicationException(message, response.OriginalException);
            }

            return response.HitsMetaData.Total > 0;
        }

        protected async Task<CountResult> CountAsync(IRepositoryQuery query) {
            if (query == null)
                throw new ArgumentNullException(nameof(query));

            var result = await GetCachedQueryResultAsync<CountResult>(query, "count").AnyContext();
            if (result != null)
                return result;

            var queryOptions = GetQueryOptions();
            await OnBeforeQueryAsync(query, queryOptions, typeof(T)).AnyContext();

            var searchDescriptor = await CreateSearchDescriptorAsync(query, queryOptions).AnyContext();
            searchDescriptor.Size(0);

            var response = await _client.SearchAsync<T>(searchDescriptor).AnyContext();
            _logger.Trace(() => response.GetRequest());

            if (!response.IsValid) {
                if (response.ApiCall.HttpStatusCode.GetValueOrDefault() == 404)
                    return new CountResult();

                string message = response.GetErrorMessage();
                _logger.Error().Exception(response.OriginalException).Message(message).Property("request", response.GetRequest()).Write();
                throw new ApplicationException(message, response.OriginalException);
            }

            result = new CountResult(response.Total, response.ToAggregations());
            await SetCachedQueryResultAsync(query, result, "count").AnyContext();
            return result;
        }

        public async Task<long> CountAsync() {
            var response = await _client.CountAsync<T>(c => c.Query(q => q.MatchAll()).Index(String.Join(",", GetIndexesByQuery(null))).Type(ElasticType.Name)).AnyContext();
            _logger.Trace(() => response.GetRequest());

            if (!response.IsValid) {
                if (response.ApiCall.HttpStatusCode.GetValueOrDefault() == 404)
                    return 0;

                string message = response.GetErrorMessage();
                _logger.Error().Exception(response.OriginalException).Message(message).Property("request", response.GetRequest()).Write();
                throw new ApplicationException(message, response.OriginalException);
            }

            return response.Count;
        }

        public Task<CountResult> CountBySearchAsync(IRepositoryQuery systemFilter, string filter = null, string aggregations = null) {
            var search = NewQuery()
                .WithSystemFilter(systemFilter)
                .WithFilter(filter)
                .WithAggregations(aggregations);

            return CountAsync(search);
        }

        protected virtual IElasticQuery NewQuery() {
            return new ElasticQuery();
        }

        public bool IsCacheEnabled { get; private set; } = true;
        protected ScopedCacheClient Cache => _scopedCacheClient ?? new ScopedCacheClient(new NullCacheClient());

        private void SetCache(ICacheClient cache) {
            IsCacheEnabled = cache != null;
            _scopedCacheClient = new ScopedCacheClient(cache ?? new NullCacheClient(), EntityTypeName);
        }

        protected void DisableCache() {
            IsCacheEnabled = false;
            _scopedCacheClient = new ScopedCacheClient(new NullCacheClient(), EntityTypeName);
        }

        protected virtual async Task InvalidateCacheAsync(IReadOnlyCollection<ModifiedDocument<T>> documents) {
            if (!IsCacheEnabled)
                return;

            if (documents != null && documents.Count > 0 && HasIdentity) {
                var keys = documents
                    .Select(d => d.Value)
                    .Cast<IIdentity>()
                    .Select(d => d.Id)
                    .ToList();

                if (keys.Count > 0)
                    await Cache.RemoveAllAsync(keys).AnyContext();
            }
        }

        public Task InvalidateCacheAsync(T document) {
            if (document == null)
                throw new ArgumentNullException(nameof(document));

            if (!IsCacheEnabled)
                return TaskHelper.Completed();

            return InvalidateCacheAsync(new[] { document });
        }

        public Task InvalidateCacheAsync(IEnumerable<T> documents) {
            var docs = documents?.ToList();
            if (docs == null || docs.Any(d => d == null))
                throw new ArgumentNullException(nameof(documents));

            if (!IsCacheEnabled)
                return TaskHelper.Completed();

            return InvalidateCacheAsync(docs.Select(d => new ModifiedDocument<T>(d, null)).ToList());
        }

        protected Task<SearchDescriptor<T>> CreateSearchDescriptorAsync(IRepositoryQuery query, IQueryOptions options) {
            return ConfigureSearchDescriptorAsync(null, query, options);
        }

        protected async Task<SearchDescriptor<T>> ConfigureSearchDescriptorAsync(SearchDescriptor<T> search, IRepositoryQuery query, IQueryOptions options) {
            if (search == null)
                search = new SearchDescriptor<T>();

            search.Type(ElasticType.Name);
            var indices = GetIndexesByQuery(query);
            if (indices?.Length > 0)
                search.Index(String.Join(",", indices));
            if (HasVersion)
                search.Version(HasVersion);

            search.IgnoreUnavailable();

            await ElasticType.QueryBuilder.ConfigureSearchAsync(query, options, search).AnyContext();

            return search;
        }

        protected virtual IQueryOptions GetQueryOptions() {
            return new ElasticQueryOptions(ElasticType) {
                DefaultExcludes = DefaultExcludes
            };
        }

        protected string[] GetIndexesByQuery(object query) {
            return HasMultipleIndexes ? TimeSeriesType.GetIndexesByQuery(query) : new[] { ElasticIndex.Name };
        }

        protected string GetIndexById(string id) {
            return HasMultipleIndexes ? TimeSeriesType.GetIndexById(id) : ElasticIndex.Name;
        }

        protected Func<T, string> GetParentIdFunc => HasParent ? d => ChildType.GetParentId(d) : (Func<T, string>)null;
        protected Func<T, string> GetDocumentIndexFunc => HasMultipleIndexes ? d => TimeSeriesType.GetDocumentIndex(d) : (Func<T, string>)(d => ElasticIndex.Name);

        protected async Task<TResult> GetCachedQueryResultAsync<TResult>(object query, string cachePrefix = null, string cacheSuffix = null) {
            var cachedQuery = query as ICachableQuery;
            if (!IsCacheEnabled || cachedQuery == null || !cachedQuery.ShouldUseCache())
                return default(TResult);

            string cacheKey = cachePrefix != null ? cachePrefix + ":" + cachedQuery.CacheKey : cachedQuery.CacheKey;
            cacheKey = cacheSuffix != null ? cacheKey + ":" + cacheSuffix : cacheKey;
            var result = await Cache.GetAsync<TResult>(cacheKey, default(TResult)).AnyContext();
            _logger.Trace(() => $"Cache {(result != null ? "hit" : "miss")}: type={ElasticType.Name} key={cacheKey}");

            return result;
        }

        protected async Task SetCachedQueryResultAsync<TResult>(object query, TResult result, string cachePrefix = null, string cacheSuffix = null) {
            var cachedQuery = query as ICachableQuery;
            if (!IsCacheEnabled || result == null || cachedQuery == null || !cachedQuery.ShouldUseCache())
                return;

            string cacheKey = cachePrefix != null ? cachePrefix + ":" + cachedQuery.CacheKey : cachedQuery.CacheKey;
            cacheKey = cacheSuffix != null ? cacheKey + ":" + cacheSuffix : cacheKey;
            await Cache.SetAsync(cacheKey, result, cachedQuery.GetCacheExpirationDateUtc() ?? SystemClock.UtcNow.AddSeconds(ElasticType.DefaultCacheExpirationSeconds)).AnyContext();
            _logger.Trace(() => $"Set cache: type={ElasticType.Name} key={cacheKey}");
        }

        #region Elastic Type Configuration

        protected IIndex ElasticIndex => ElasticType.Index;

        private IIndexType<T> _elasticType;

        protected IIndexType<T> ElasticType {
            get { return _elasticType; }
            private set {
                _elasticType = value;

                if (_elasticType is IChildIndexType<T>) {
                    HasParent = true;
                    ChildType = _elasticType as IChildIndexType<T>;
                } else {
                    HasParent = false;
                    ChildType = null;
                }

                if (_elasticType is ITimeSeriesIndexType) {
                    HasMultipleIndexes = true;
                    TimeSeriesType = _elasticType as ITimeSeriesIndexType<T>;
                } else {
                    HasMultipleIndexes = false;
                    TimeSeriesType = null;
                }
            }
        }

        protected bool HasParent { get; private set; }
        protected IChildIndexType<T> ChildType { get; private set; }
        protected bool HasMultipleIndexes { get; private set; }
        protected ITimeSeriesIndexType<T> TimeSeriesType { get; private set; }

        #endregion

        protected string GetPropertyName(string propertyName) {
            return _client.Infer.PropertyName(typeof(T).GetProperty(propertyName));
        }

        #region Events

        public AsyncEvent<BeforeQueryEventArgs<T>> BeforeQuery { get; } = new AsyncEvent<BeforeQueryEventArgs<T>>();

        private async Task OnBeforeQueryAsync(IRepositoryQuery query, IQueryOptions options, Type resultType) {
            var identityQuery = query as IIdentityQuery;
            if (SupportsSoftDeletes && IsCacheEnabled && identityQuery != null) {
                var deletedIds = await Cache.GetSetAsync<string>("deleted").AnyContext();
                if (deletedIds.HasValue)
                    identityQuery.WithExcludedIds(deletedIds.Value);
            }

            if (BeforeQuery == null)
                return;

            await BeforeQuery.InvokeAsync(this, new BeforeQueryEventArgs<T>(query, options, this, resultType)).AnyContext();
        }

        #endregion
    }
}