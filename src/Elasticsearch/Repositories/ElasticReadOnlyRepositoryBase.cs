using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Elasticsearch.Net;
using Foundatio.Caching;
using Foundatio.Repositories.Elasticsearch.Queries;
using Foundatio.Logging;
using Foundatio.Repositories.Elasticsearch.Configuration;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Elasticsearch.Models;
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Elasticsearch.Queries.Options;
using Foundatio.Repositories.Elasticsearch.Repositories;
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

        protected readonly ILogger _logger;
        protected readonly IElasticClient _client;
        protected readonly IElasticQueryBuilder _queryBuilder = ElasticQueryBuilder.Default;

        private ScopedCacheClient _scopedCacheClient;

        protected ElasticReadOnlyRepositoryBase(IElasticClient client) : this(client, null, null) { }

        protected ElasticReadOnlyRepositoryBase(IElasticClient client, ICacheClient cache, ILogger logger) {
            _client = client;
            SetCache(cache);
            _logger = logger ?? NullLogger.Instance;
        }

        protected Task<IFindResults<T>> FindAsync(IRepositoryQuery query) {
            return FindAsAsync<T>(query);
        }

        protected ISet<string> DefaultExcludes { get; } = new HashSet<string>();

        protected async Task<IFindResults<TResult>> FindAsAsync<TResult>(IRepositoryQuery query) where TResult : class, new() {
            if (query == null)
                throw new ArgumentNullException(nameof(query));

            var pagableQuery = query as IPagableQuery;
            var pagingOptions = pagableQuery?.Options as IPagingOptions;
            var elasticPagingOptions = pagableQuery?.Options as ElasticPagingOptions;
            bool useSnapshotPaging = elasticPagingOptions?.UseSnapshotPaging ?? false;

            // don't use caching with snapshot paging.
            bool allowCaching = IsCacheEnabled && (elasticPagingOptions == null || elasticPagingOptions.UseSnapshotPaging == false);

            await OnBeforeQueryAsync(query, typeof(TResult)).AnyContext();

            Func<IFindResults<TResult>, Task<IFindResults<TResult>>> getNextPageFunc = async r => {
                var previousResults = r as IElasticFindResults<TResult>;
                if (previousResults == null)
                    throw new ArgumentException(nameof(r));

                if (!String.IsNullOrEmpty(previousResults.ScrollId)) {
                    var scrollResponse = await _client.ScrollAsync<TResult>(pagableQuery.GetLifetime(), previousResults.ScrollId).AnyContext();
                    _logger.Trace(() => scrollResponse.GetRequest());

                    var results = scrollResponse.ToFindResults();
                    results.Page = previousResults.Page + 1;
                    results.HasMore = scrollResponse.Hits.Count() >= pagableQuery.GetLimit();
                    return results;
                }

                if (pagableQuery == null)
                    return new ElasticFindResults<TResult>();

                if (pagingOptions != null)
                    pagingOptions.Page = pagingOptions.Page == null ? 2 : pagingOptions.Page + 1;

                return await FindAsAsync<TResult>(query).AnyContext();
            };

            string cacheSuffix = pagableQuery?.ShouldUseLimit() == true ? pagingOptions.Page?.ToString() ?? "1" : String.Empty;

            ElasticFindResults<TResult> result;
            if (allowCaching) {
                result = await GetCachedQueryResultAsync<ElasticFindResults<TResult>>(query, cacheSuffix: cacheSuffix).AnyContext();
                if (result != null) {
                    ((IGetNextPage<TResult>)result).GetNextPageFunc = async r => await getNextPageFunc(r).AnyContext();
                    return result;
                }
            }

            ISearchResponse<TResult> response = null;

            if (useSnapshotPaging == false || String.IsNullOrEmpty(elasticPagingOptions?.ScrollId)) {
                SearchDescriptor<T> searchDescriptor = CreateSearchDescriptor(query);
                if (useSnapshotPaging)
                    searchDescriptor.SearchType(SearchType.Scan).Scroll(pagableQuery.GetLifetime());

                response = await _client.SearchAsync<TResult>(searchDescriptor).AnyContext();
                _logger.Trace(() => response.GetRequest());
                if (!response.IsValid) {
                    if (response.ConnectionStatus.HttpStatusCode.GetValueOrDefault() == 404)
                        return new ElasticFindResults<TResult>();

                    string message = response.GetErrorMessage();
                    _logger.Error().Exception(response.ConnectionStatus.OriginalException).Message(message).Property("request", response.GetRequest()).Write();
                    throw new ApplicationException(message, response.ConnectionStatus.OriginalException);
                }
            }

            if (useSnapshotPaging) {
                var scrollResponse = await _client.ScrollAsync<TResult>(pagableQuery.GetLifetime(), response?.ScrollId ?? elasticPagingOptions?.ScrollId).AnyContext();
                _logger.Trace(() => scrollResponse.GetRequest());

                if (!scrollResponse.IsValid) {
                    string message = scrollResponse.GetErrorMessage();
                    _logger.Error().Exception(scrollResponse.ConnectionStatus.OriginalException).Message(message).Property("request", scrollResponse.GetRequest()).Write();
                    throw new ApplicationException(message, scrollResponse.ConnectionStatus.OriginalException);
                }

                result = scrollResponse.ToFindResults();
                result.HasMore = scrollResponse.Hits.Count() >= pagableQuery.GetLimit();

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

        protected async Task<T> FindOneAsync(IRepositoryQuery query) {
            if (query == null)
                throw new ArgumentNullException(nameof(query));

            var result = IsCacheEnabled ? await GetCachedQueryResultAsync<T>(query).AnyContext() : null;
            if (result != null)
                return result;

            await OnBeforeQueryAsync(query, typeof(T)).AnyContext();

            var searchDescriptor = CreateSearchDescriptor(query).Size(1);
            var response = await _client.SearchAsync<T>(searchDescriptor).AnyContext();
            _logger.Trace(() => response.GetRequest());

            if (!response.IsValid) {
                if (response.ConnectionStatus.HttpStatusCode.GetValueOrDefault() == 404)
                    return null;

                string message = response.GetErrorMessage();
                _logger.Error().Exception(response.ConnectionStatus.OriginalException).Message(message).Property("request", response.GetRequest()).Write();
                throw new ApplicationException(message, response.ConnectionStatus.OriginalException);
            }

            // Ensure document version is set.
            result = response.Hits.ToFindHits().FirstOrDefault()?.Document;
            if (IsCacheEnabled)
                await SetCachedQueryResultAsync(query, result).AnyContext();

            return result;
        }

        public Task<IFindResults<T>> SearchAsync(IRepositoryQuery systemFilter, string filter = null, string criteria = null, SortingOptions sorting = null, PagingOptions paging = null, AggregationOptions aggregations = null) {
            var search = NewQuery()
                .WithSystemFilter(systemFilter)
                .WithFilter(filter)
                .WithSearchQuery(criteria, false)
                .WithAggregation(aggregations)
                .WithSort(sorting)
                .WithPaging(paging);

            return FindAsync(search);
        }

        public async Task<T> GetByIdAsync(string id, bool useCache = false, TimeSpan? expiresIn = null) {
            if (String.IsNullOrEmpty(id))
                return null;

            ElasticFindHit<T> hit = null;
            if (IsCacheEnabled && useCache)
                hit = await Cache.GetAsync<ElasticFindHit<T>>(id, null).AnyContext();

            if (hit?.Document != null) {
                _logger.Trace(() => $"Cache hit: type={ElasticType.Name} key={id}");
                return hit.Document;
            }

            string index = GetIndexById(id);
            if (!HasParent) {
                var res = await _client.GetAsync<T>(id, index, ElasticType.Name).AnyContext();
                _logger.Trace(() => res.GetRequest());

                hit = res.ToFindHit();
            } else {
                // we don't have the parent id so we have to do a query
                var document = await FindOneAsync(NewQuery().WithId(id)).AnyContext();
                if (document != null) {
                    hit = new ElasticFindHit<T> {
                        Id = id,
                        Document = document,
                        Version = HasVersion ? ((IVersioned)document).Version : (long?)null,
                        Type = ElasticType.Name
                    };
                }
            }

            if (IsCacheEnabled && hit != null && useCache)
                await Cache.SetAsync(id, hit, expiresIn ?? TimeSpan.FromSeconds(RepositoryConstants.DEFAULT_CACHE_EXPIRATION_SECONDS)).AnyContext();

            return hit?.Document;
        }

        public async Task<IFindResults<T>> GetByIdsAsync(IEnumerable<string> ids, bool useCache = false, TimeSpan? expiresIn = null) {
            var hits = new List<ElasticFindHit<T>>();
            var idList = ids?.Distinct().Where(i => !String.IsNullOrEmpty(i)).ToList();

            if (idList == null || idList.Count == 0)
                return new ElasticFindResults<T>();

            if (!HasIdentity)
                throw new NotSupportedException("Model type must implement IIdentity.");

            if (IsCacheEnabled && useCache) {
                var cacheHits = await Cache.GetAllAsync<ElasticFindHit<T>>(idList).AnyContext();
                hits.AddRange(cacheHits.Where(kvp => kvp.Value.HasValue).Select(kvp => kvp.Value.Value));

                var notCachedIds = idList.Except(hits.Select(i => i.Id)).ToArray();
                if (notCachedIds.Length == 0)
                    return new ElasticFindResults<T>(hits, hits.Count);
            }

            var itemsToFind = new List<string>(idList.Except(hits.Select(i => i.Id)));
            var multiGet = new MultiGetDescriptor();

            if (!HasParent) {
                itemsToFind.ForEach(id => multiGet.Get<T>(f => f.Id(id).Index(GetIndexById(id)).Type(ElasticType.Name)));

                var multiGetResults = await _client.MultiGetAsync(multiGet).AnyContext();
                _logger.Trace(() => multiGetResults.GetRequest());

                foreach (var doc in multiGetResults.Documents) {
                    if (!doc.Found)
                        continue;

                    hits.Add(((IMultiGetHit<T>)doc).ToFindHit());
                    itemsToFind.Remove(doc.Id);
                }
            }

            // fallback to doing a find
            if (itemsToFind.Count > 0 && (HasParent || HasMultipleIndexes))
                hits.AddRange((await FindAsync(NewQuery().WithIds(itemsToFind)).AnyContext()).Hits.Cast<ElasticFindHit<T>>());

            if (IsCacheEnabled && useCache) {
                foreach (var item in hits)
                    await Cache.SetAsync(item.Id, item, expiresIn.HasValue ? SystemClock.UtcNow.Add(expiresIn.Value) : SystemClock.UtcNow.AddSeconds(ElasticType.DefaultCacheExpirationSeconds)).AnyContext();
            }

            return new ElasticFindResults<T>(hits, hits.Count);
        }

        public Task<IFindResults<T>> GetAllAsync(SortingOptions sorting = null, PagingOptions paging = null) {
            var search = NewQuery()
                .WithPaging(paging)
                .WithSort(sorting);

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

            var searchDescriptor = CreateSearchDescriptor(query).Size(1);
            searchDescriptor.Fields("id");
            var response = await _client.SearchAsync<T>(searchDescriptor).AnyContext();
            _logger.Trace(() => response.GetRequest());

            if (!response.IsValid) {
                if (response.ConnectionStatus.HttpStatusCode.GetValueOrDefault() == 404)
                    return false;

                string message = response.GetErrorMessage();
                _logger.Error().Exception(response.ConnectionStatus.OriginalException).Message(message).Property("request", response.GetRequest()).Write();
                throw new ApplicationException(message, response.ConnectionStatus.OriginalException);
            }

            return response.HitsMetaData.Total > 0;
        }

        protected async Task<CountResult> CountAsync(IRepositoryQuery query) {
            if (query == null)
                throw new ArgumentNullException(nameof(query));

            var result = await GetCachedQueryResultAsync<CountResult>(query, "count").AnyContext();
            if (result != null)
                return result;

            var searchDescriptor = CreateSearchDescriptor(query);
            searchDescriptor.SearchType(SearchType.Count);

            var response = await _client.SearchAsync<T>(searchDescriptor).AnyContext();
            _logger.Trace(() => response.GetRequest());

            if (!response.IsValid) {
                if (response.ConnectionStatus.HttpStatusCode.GetValueOrDefault() == 404)
                    return new CountResult();

                string message = response.GetErrorMessage();
                _logger.Error().Exception(response.ConnectionStatus.OriginalException).Message(message).Property("request", response.GetRequest()).Write();
                throw new ApplicationException(message, response.ConnectionStatus.OriginalException);
            }

            result = new CountResult(response.Total, response.ToAggregationResult());

            await SetCachedQueryResultAsync(query, result, "count").AnyContext();

            return result;
        }

        public async Task<long> CountAsync() {
            var response = await _client.CountAsync<T>(c => c.Query(q => q.MatchAll()).Indices(GetIndexesByQuery(null)).Type(ElasticType.Name)).AnyContext();
            _logger.Trace(() => response.GetRequest());

            if (!response.IsValid) {
                if (response.ConnectionStatus.HttpStatusCode.GetValueOrDefault() == 404)
                    return 0;

                string message = response.GetErrorMessage();
                _logger.Error().Exception(response.ConnectionStatus.OriginalException).Message(message).Property("request", response.GetRequest()).Write();
                throw new ApplicationException(message, response.ConnectionStatus.OriginalException);
            }

            return response.Count;
        }

        public Task<CountResult> CountBySearchAsync(IRepositoryQuery systemFilter, string filter = null, AggregationOptions aggregations = null) {
            var search = NewQuery()
                .WithSystemFilter(systemFilter)
                .WithFilter(filter)
                .WithAggregation(aggregations);

            return CountAsync(search);
        }

        public async Task<IReadOnlyCollection<AggregationResult>> GetAggregationsAsync(IRepositoryQuery query) {
            var aggregationQuery = query as IAggregationQuery;

            if (aggregationQuery == null || aggregationQuery.AggregationFields.Count == 0)
                throw new ArgumentException("Query must contain aggregation fields.", nameof(query));

            if (ElasticType.AllowedAggregationFields.Count > 0 && !aggregationQuery.AggregationFields.All(f => ElasticType.AllowedAggregationFields.Contains(f.Field)))
                throw new ArgumentException("All aggregation fields must be allowed.", nameof(query));

            var searchDescriptor = CreateSearchDescriptor(query).SearchType(SearchType.Count);
            var response = await _client.SearchAsync<T>(searchDescriptor).AnyContext();
            _logger.Trace(() => response.GetRequest());

            if (!response.IsValid) {
                string message = $"Retrieving aggregations failed: {response.GetErrorMessage()}";
                _logger.Error().Exception(response.ConnectionStatus.OriginalException).Message(message).Property("request", response.GetRequest()).Write();
                throw new ApplicationException(message, response.ConnectionStatus.OriginalException);
            }

            return response.ToAggregationResult();
        }

        public Task<IReadOnlyCollection<AggregationResult>> GetAggregationsAsync(IRepositoryQuery systemFilter, AggregationOptions aggregations, string filter = null) {
            var search = NewQuery()
                .WithSystemFilter(systemFilter)
                .WithFilter(filter)
                .WithAggregation(aggregations);

            return GetAggregationsAsync(search);
        }

        protected IElasticQuery NewQuery() {
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

        protected SearchDescriptor<T> CreateSearchDescriptor(IRepositoryQuery query) {
            return ConfigureSearchDescriptor(null, query);
        }

        protected SearchDescriptor<T> ConfigureSearchDescriptor(SearchDescriptor<T> search, IRepositoryQuery query) {
            if (search == null)
                search = new SearchDescriptor<T>();

            search.Type(ElasticType.Name);
            var indices = GetIndexesByQuery(query);
            if (indices?.Length > 0)
                search.Indices(indices);
            if (HasVersion)
                search.Version(HasVersion);

            search.IgnoreUnavailable();

            _queryBuilder.ConfigureSearch(query, GetQueryOptions(), search);

            return search;
        }

        protected virtual IQueryOptions GetQueryOptions() {
            return new ElasticQueryOptions(ElasticType) {
                SupportsSoftDeletes = SupportsSoftDeletes,
                HasIdentity = HasIdentity,
                DefaultExcludes = DefaultExcludes.ToArray()
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
            await Cache.SetAsync(cacheKey, result, cachedQuery.GetCacheExpirationDateUtc()).AnyContext();
            _logger.Trace(() => $"Set cache: type={ElasticType.Name} key={cacheKey}");
        }

        #region Elastic Type Configuration

        protected IIndex ElasticIndex => ElasticType.Index;

        private IIndexType<T> _elasticType;

        protected IIndexType<T> ElasticType {
            get { return _elasticType; }
            set {
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

        #region Events

        public AsyncEvent<BeforeQueryEventArgs<T>> BeforeQuery { get; } = new AsyncEvent<BeforeQueryEventArgs<T>>();

        private async Task OnBeforeQueryAsync(IRepositoryQuery query, Type resultType) {
            var identityQuery = query as IIdentityQuery;
            if (SupportsSoftDeletes && IsCacheEnabled && identityQuery != null) {
                var deletedIds = await Cache.GetSetAsync<string>("deleted").AnyContext();
                if (deletedIds.HasValue)
                    identityQuery.WithExcludedIds(deletedIds.Value);
            }

            if (BeforeQuery == null)
                return;

            await BeforeQuery.InvokeAsync(this, new BeforeQueryEventArgs<T>(query, this, resultType)).AnyContext();
        }

        #endregion
    }
}