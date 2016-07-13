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
using Foundatio.Repositories.Elasticsearch.Queries.Builders;
using Foundatio.Repositories.Extensions;
using Foundatio.Repositories.Models;
using Foundatio.Repositories.Queries;
using Foundatio.Repositories.Utility;
using Nest;

namespace Foundatio.Repositories.Elasticsearch {
    public abstract class ElasticReadOnlyRepositoryBase<T> : ISearchableRepository<T> where T : class, new() {
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

        protected ElasticReadOnlyRepositoryBase(IElasticClient client) : this(client, null, null) {}

        protected ElasticReadOnlyRepositoryBase(IElasticClient client, ICacheClient cache, ILogger logger) {
            _client = client;
            SetCache(cache);
            _logger = logger ?? NullLogger.Instance;
        }

        protected Task<FindResults<T>> FindAsync(object query) {
            return FindAsAsync<T>(query);
        }

        protected async Task<FindResults<TResult>> FindAsAsync<TResult>(object query) where TResult : class, new() {
            if (query == null)
                throw new ArgumentNullException(nameof(query));

            var pagableQuery = query as IPagableQuery;
            // don't use caching with snapshot paging.
            bool allowCaching = IsCacheEnabled && (pagableQuery == null || pagableQuery.UseSnapshotPaging == false);

            Func<FindResults<TResult>, Task<FindResults<TResult>>> getNextPageFunc = async r => {
                if (!String.IsNullOrEmpty(r.ScrollId)) {
                    var scrollResponse = await _client.ScrollAsync<TResult>(pagableQuery.GetLifetime(), r.ScrollId).AnyContext();
                    _logger.Trace(() => scrollResponse.GetRequest());
                    return scrollResponse.ToFindResults(HasVersion, pagableQuery != null && pagableQuery.UseSnapshotPaging ? Int32.MaxValue : pagableQuery?.Limit);
                }

                if (pagableQuery == null)
                    return new FindResults<TResult>();

                pagableQuery.Page = pagableQuery.Page == null ? 2 : pagableQuery.Page + 1;
                return await FindAsAsync<TResult>(query).AnyContext();
            };

            string cacheSuffix = pagableQuery?.ShouldUseLimit() == true ? pagableQuery.Page?.ToString() ?? "1" : String.Empty;

            FindResults<TResult> result;
            if (allowCaching) {
                result = await GetCachedQueryResultAsync<FindResults<TResult>>(query, cacheSuffix: cacheSuffix).AnyContext();
                if (result != null) {
                    ((IGetNextPage<TResult>)result).GetNextPageFunc = getNextPageFunc;
                    return result;
                }
            }

            SearchDescriptor<T> searchDescriptor = CreateSearchDescriptor(query);
            if (pagableQuery?.UseSnapshotPaging == true)
                searchDescriptor.SearchType(SearchType.Scan).Scroll(pagableQuery.GetLifetime());

            var response = await _client.SearchAsync<TResult>(searchDescriptor).AnyContext();
            _logger.Trace(() => response.GetRequest());
            if (!response.IsValid) {
                _logger.Error().Message($"Elasticsearch error code \"{response.ConnectionStatus.HttpStatusCode}\"").Property("request", response.GetRequest()).Write();
                throw new ApplicationException($"Elasticsearch error code \"{response.ConnectionStatus.HttpStatusCode}\".", response.ConnectionStatus.OriginalException);
            }

            if (pagableQuery?.UseSnapshotPaging == true) {
                var scrollResponse = await _client.ScrollAsync<TResult>(pagableQuery.GetLifetime(), response.ScrollId).AnyContext();
                _logger.Trace(() => scrollResponse.GetRequest());
                if (!scrollResponse.IsValid) {
                    _logger.Error().Message($"Elasticsearch error code \"{scrollResponse.ConnectionStatus.HttpStatusCode}\"").Property("request", scrollResponse.GetRequest()).Write();
                    throw new ApplicationException($"Elasticsearch error code \"{scrollResponse.ConnectionStatus.HttpStatusCode}\".", scrollResponse.ConnectionStatus.OriginalException);
                }

                result = scrollResponse.ToFindResults(HasVersion);
                ((IGetNextPage<TResult>)result).GetNextPageFunc = getNextPageFunc;
            } else if (pagableQuery?.ShouldUseLimit() == true) {
                result = response.ToFindResults(HasVersion, pagableQuery.Limit);
                ((IGetNextPage<TResult>)result).GetNextPageFunc = getNextPageFunc;
            } else {
                result = response.ToFindResults(HasVersion);
            }

            if (!allowCaching)
                return result;

            var nextPageFunc = ((IGetNextPage<TResult>)result).GetNextPageFunc;
            ((IGetNextPage<TResult>)result).GetNextPageFunc = null;
            await SetCachedQueryResultAsync(query, result, cacheSuffix: cacheSuffix).AnyContext();
            ((IGetNextPage<TResult>)result).GetNextPageFunc = nextPageFunc;

            return result;
        }

        protected async Task<T> FindOneAsync(object query) {
            if (query == null)
                throw new ArgumentNullException(nameof(query));
            
            var result = IsCacheEnabled ? await GetCachedQueryResultAsync<T>(query).AnyContext() : null;
            if (result != null)
                return result;

            var searchDescriptor = CreateSearchDescriptor(query).Size(1);
            var response = await _client.SearchAsync<T>(searchDescriptor).AnyContext();
            _logger.Trace(() => response.GetRequest());
            result = response.Documents.FirstOrDefault();

            if (IsCacheEnabled)
                await SetCachedQueryResultAsync(query, result).AnyContext();

            return result;
        }

        public async Task<bool> ExistsAsync(string id) {
            if (String.IsNullOrEmpty(id))
                return false;

            return await ExistsAsync(new Query().WithId(id)).AnyContext();
        }

        protected async Task<bool> ExistsAsync(object query) {
            if (query == null)
                throw new ArgumentNullException(nameof(query));

            var searchDescriptor = CreateSearchDescriptor(query).Size(1);
            searchDescriptor.Fields("id");
            var response = await _client.SearchAsync<T>(searchDescriptor).AnyContext();
            _logger.Trace(() => response.GetRequest());

            return response.HitsMetaData.Total > 0;
        }

        protected async Task<CountResult> CountAsync(object query) {
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
                _logger.Error().Message($"Elasticsearch error code \"{response.ConnectionStatus.HttpStatusCode}\"").Property("request", response.GetRequest()).Write();
                throw new ApplicationException($"Elasticsearch error code \"{response.ConnectionStatus.HttpStatusCode}\".", response.ConnectionStatus.OriginalException);
            }

            result = new CountResult(response.Total, response.ToAggregationResult());

            await SetCachedQueryResultAsync(query, result, "count").AnyContext();

            return result;
        }

        public async Task<long> CountAsync() {
            var response = await _client.CountAsync<T>(c => c.Query(q => q.MatchAll()).Indices(GetIndexesByQuery(null))).AnyContext();
            _logger.Trace(() => response.GetRequest());
            return response.Count;
        }

        public async Task<T> GetByIdAsync(string id, bool useCache = false, TimeSpan? expiresIn = null) {
            if (String.IsNullOrEmpty(id))
                return null;

            T result = null;
            if (IsCacheEnabled && useCache)
                result = await Cache.GetAsync<T>(id, null).AnyContext();

            if (result != null) {
                _logger.Trace(() => $"Cache hit: type={ElasticType.Name} key={id}");
                return result;
            }

            string index = GetIndexById(id);
            if (!HasParent) {
                var res = await _client.GetAsync<T>(id, index).AnyContext();
                _logger.Trace(() => res.GetRequest());

                result = res.Source;
            } else {
                // we don't have the parent id so we have to do a query
                result = await FindOneAsync(new ElasticQuery().WithId(id)).AnyContext();
            }

            if (IsCacheEnabled && result != null && useCache)
                await Cache.SetAsync(id, result, expiresIn ?? TimeSpan.FromSeconds(RepositoryConstants.DEFAULT_CACHE_EXPIRATION_SECONDS)).AnyContext();

            return result;
        }

        public async Task<FindResults<T>> GetByIdsAsync(ICollection<string> ids, bool useCache = false, TimeSpan? expiresIn = null) {
            var results = new FindResults<T>();
            if (ids == null || ids.Count == 0)
                return results;

            if (!HasIdentity)
                throw new NotSupportedException("Model type must implement IIdentity.");

            if (IsCacheEnabled && useCache) {
                var cacheHits = await Cache.GetAllAsync<T>(ids.Distinct()).AnyContext();
                results.Documents.AddRange(cacheHits.Where(kvp => kvp.Value.HasValue).Select(kvp => kvp.Value.Value));
                results.Total = results.Documents.Count;

                var notCachedIds = ids.Except(results.Documents.Select(i => ((IIdentity)i).Id)).ToArray();
                if (notCachedIds.Length == 0)
                    return results;
            }

            var itemsToFind = new List<string>(ids.Distinct().Except(results.Documents.Select(i => ((IIdentity)i).Id)));
            var multiGet = new MultiGetDescriptor();

            if (!HasParent) {
                itemsToFind.ForEach(id => multiGet.Get<T>(f => f.Id(id).Index(GetIndexById(id))));

                var multiGetResults = await _client.MultiGetAsync(multiGet).AnyContext();
                _logger.Trace(() => multiGetResults.GetRequest());

                foreach (var doc in multiGetResults.Documents) {
                    if (!doc.Found)
                        continue;

                    results.Documents.Add(doc.Source as T);
                    itemsToFind.Remove(doc.Id);
                }
            }

            // fallback to doing a find
            if (itemsToFind.Count > 0 && (HasParent || HasMultipleIndexes))
                results.Documents.AddRange((await FindAsync(new ElasticQuery().WithIds(itemsToFind)).AnyContext()).Documents);

            if (IsCacheEnabled && useCache) {
                foreach (var item in results.Documents)
                    await Cache.SetAsync(((IIdentity)item).Id, item, expiresIn.HasValue ? DateTime.UtcNow.Add(expiresIn.Value) : DateTime.UtcNow.AddSeconds(ElasticType.DefaultCacheExpirationSeconds)).AnyContext();
            }

            results.Total = results.Documents.Count;
            return results;
        }

        public Task<FindResults<T>> GetAllAsync(SortingOptions sorting = null, PagingOptions paging = null) {
            var search = new ElasticQuery()
                .WithPaging(paging)
                .WithSort(sorting);

            return FindAsync(search);
        }

        public Task<CountResult> CountAsync(object systemFilter, string userFilter = null, string query = null, AggregationOptions aggregations = null) {
            var search = new ElasticQuery()
                .WithSystemFilter(systemFilter)
                .WithFilter(userFilter)
                .WithSearchQuery(query, false)
                .WithAggregation(aggregations);

            return CountAsync(search);
        }

        public Task<FindResults<T>> SearchAsync(object systemFilter, string userFilter = null, string query = null, SortingOptions sorting = null, PagingOptions paging = null, AggregationOptions aggregations = null) {
            var search = new ElasticQuery()
                .WithSystemFilter(systemFilter)
                .WithFilter(userFilter)
                .WithSearchQuery(query, false)
                .WithAggregation(aggregations)
                .WithSort(sorting)
                .WithPaging(paging);

            return FindAsync(search);
        }

        public async Task<ICollection<AggregationResult>> GetAggregationsAsync(object query) {
            var aggregationQuery = query as IAggregationQuery;

            if (aggregationQuery == null || aggregationQuery.AggregationFields.Count == 0)
                throw new ArgumentException("Query must contain aggregation fields.", nameof(query));

            if (ElasticType.AllowedAggregationFields.Count > 0 && !aggregationQuery.AggregationFields.All(f => ElasticType.AllowedAggregationFields.Contains(f.Field)))
                throw new ArgumentException("All aggregation fields must be allowed.", nameof(query));

            var search = CreateSearchDescriptor(query).SearchType(SearchType.Count);
            var res = await _client.SearchAsync<T>(search);
            _logger.Trace(() => res.GetRequest());

            if (!res.IsValid) {
                _logger.Error().Message("Retrieving aggregations failed: {0}", res.ServerError.Error).Write();
                throw new ApplicationException("Retrieving aggregations failed.");
            }

            return res.ToAggregationResult();
        }

        public Task<ICollection<AggregationResult>> GetAggregationsAsync(object systemFilter, AggregationOptions aggregations, string userFilter = null, string query = null) {
            var search = new ElasticQuery()
                .WithSystemFilter(systemFilter)
                .WithFilter(userFilter)
                .WithSearchQuery(query, false)
                .WithAggregation(aggregations);

            return GetAggregationsAsync(search);
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
        
        protected virtual async Task InvalidateCacheAsync(ICollection<ModifiedDocument<T>> documents) {
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
            if (!IsCacheEnabled)
                return TaskHelper.Completed();

            return InvalidateCacheAsync(new[] { document });
        }

        public Task InvalidateCacheAsync(ICollection<T> documents) {
            if (!IsCacheEnabled)
                return TaskHelper.Completed();

            return InvalidateCacheAsync(documents.Select(d => new ModifiedDocument<T>(d, null)).ToList());
        }

        protected SearchDescriptor<T> CreateSearchDescriptor(object query) {
            return ConfigureSearchDescriptor(null, query);
        }

        protected SearchDescriptor<T> ConfigureSearchDescriptor(SearchDescriptor<T> search, object query) {
            if (search == null)
                search = new SearchDescriptor<T>();

            var indices = GetIndexesByQuery(query);
            if (indices?.Length > 0)
                search.Indices(indices);
            search.IgnoreUnavailable();

            _queryBuilder.BuildSearch(query, this, ref search);

            return search;
        }

        protected string[] GetIndexesByQuery(object query) {
            return !HasMultipleIndexes ? new[] { ElasticIndex.Name } : TimeSeriesType.GetIndexesByQuery(query);
        }

        protected string GetIndexById(string id) {
            return !HasMultipleIndexes ? ElasticIndex.Name : TimeSeriesType.GetIndexById(id);
        }

        protected string GetDocumentIndex(T document) {
            return !HasMultipleIndexes ? ElasticIndex.Name : TimeSeriesType.GetDocumentIndex(document);
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
        protected IIndexType<T> ElasticType
        {
            get
            {
                return _elasticType;
            }
            set
            {
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
    }
}