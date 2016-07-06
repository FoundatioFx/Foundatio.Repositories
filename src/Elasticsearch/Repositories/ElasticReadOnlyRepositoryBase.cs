using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Elasticsearch.Net;
using Foundatio.Caching;
using Foundatio.Repositories.Elasticsearch.Queries;
using Foundatio.Extensions;
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
        protected readonly bool _hasIdentity = typeof(IIdentity).IsAssignableFrom(typeof(T));
        protected readonly bool _hasDates = typeof(IHaveDates).IsAssignableFrom(typeof(T));
        protected readonly bool _hasCreatedDate = typeof(IHaveCreatedDate).IsAssignableFrom(typeof(T));
        protected readonly bool _supportsSoftDeletes = typeof(ISupportSoftDeletes).IsAssignableFrom(typeof(T));
        protected readonly IChildIndexType<T> _childIndexType = null;
        protected readonly ITimeSeriesIndexType<T> _timeSeriesIndexType = null;
        protected readonly bool _hasParent;
        protected readonly bool _hasMultipleIndexes;

        protected readonly ILogger _logger;
        private ScopedCacheClient _scopedCacheClient;

        protected ElasticReadOnlyRepositoryBase(IElasticRepositoryConfiguration<T> configuration, ILoggerFactory loggerFactory = null) {
            Configuration = configuration;

            if (configuration.Type is IChildIndexType<T>) {
                _hasParent = true;
                _childIndexType = configuration.Type as IChildIndexType<T>;
            }

            if (configuration.Type is ITimeSeriesIndexType) {
                _hasMultipleIndexes = true;
                _timeSeriesIndexType = configuration.Type as ITimeSeriesIndexType<T>;
            }

            _logger = loggerFactory?.CreateLogger(GetType()) ?? NullLogger.Instance;
        }

        protected IElasticRepositoryConfiguration<T> Configuration { get; }

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
                    var scrollResponse = await Configuration.Client.ScrollAsync<TResult>("2m", r.ScrollId).AnyContext();
                    return new FindResults<TResult> {
                        Documents = scrollResponse.Documents.ToList(),
                        Total = r.Total,
                        ScrollId = r.ScrollId
                    };
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
                    result.GetNextPageFunc = getNextPageFunc;
                    return result;
                }
            }

            SearchDescriptor<T> searchDescriptor = CreateSearchDescriptor(query);
            if (pagableQuery?.UseSnapshotPaging == true)
                searchDescriptor.SearchType(SearchType.Scan).Scroll("2m");

            var response = await Configuration.Client.SearchAsync<TResult>(searchDescriptor).AnyContext();
            if (!response.IsValid)
                throw new ApplicationException($"Elasticsearch error code \"{response.ConnectionStatus.HttpStatusCode}\".", response.ConnectionStatus.OriginalException);

            if (pagableQuery?.UseSnapshotPaging == true) {
                var scanResponse = response;
                response = await Configuration.Client.ScrollAsync<TResult>("2m", response.ScrollId).AnyContext();
                if (!response.IsValid)
                    throw new ApplicationException($"Elasticsearch error code \"{response.ConnectionStatus.HttpStatusCode}\".", response.ConnectionStatus.OriginalException);

                result = new FindResults<TResult> {
                    Documents = response.Documents.ToList(),
                    Total = scanResponse.Total,
                    ScrollId = scanResponse.ScrollId,
                    GetNextPageFunc = getNextPageFunc
                };
            } else if (pagableQuery?.ShouldUseLimit() == true) {
                result = new FindResults<TResult> {
                    Documents = response.Documents.Take(pagableQuery.GetLimit()).ToList(),
                    Total = response.Total,
                    HasMore = pagableQuery.ShouldUseLimit() && response.Documents.Count() > pagableQuery.GetLimit(),
                    GetNextPageFunc = getNextPageFunc
                };
            } else {
                result = new FindResults<TResult> {
                    Documents = response.Documents.ToList(),
                    Total = response.Total
                };
            }

            result.Facets = response.ToFacetResults();

            if (allowCaching) {
                var nextPageFunc = result.GetNextPageFunc;
                result.GetNextPageFunc = null;
                await SetCachedQueryResultAsync(query, result, cacheSuffix: cacheSuffix).AnyContext();
                result.GetNextPageFunc = nextPageFunc;
            }

            return result;
        }

        protected async Task<T> FindOneAsync(object query) {
            if (query == null)
                throw new ArgumentNullException(nameof(query));
            
            var result = IsCacheEnabled ? await GetCachedQueryResultAsync<T>(query).AnyContext() : null;
            if (result != null)
                return result;

            var searchDescriptor = CreateSearchDescriptor(query).Size(1);
            result = (await Configuration.Client.SearchAsync<T>(searchDescriptor).AnyContext()).Documents.FirstOrDefault();

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

            return (await Configuration.Client.SearchAsync<T>(searchDescriptor).AnyContext()).HitsMetaData.Total > 0;
        }

        protected async Task<long> CountAsync(object query) {
            if (query == null)
                throw new ArgumentNullException(nameof(query));

            var result = IsCacheEnabled ? await GetCachedQueryResultAsync<long?>(query, "count").AnyContext() : null;
            if (result != null)
                return result.Value;

            var countDescriptor = new CountDescriptor<T>().Query(Configuration.QueryBuilder.CreateQuery<T>(query, Configuration));
            var indices = GetIndexesByQuery(query);
            if (indices?.Length > 0)
                countDescriptor.Indices(indices);
            countDescriptor.IgnoreUnavailable();

            var results = await Configuration.Client.CountAsync<T>(countDescriptor).AnyContext();
            if (!results.IsValid)
                throw new ApplicationException($"ElasticSearch error code \"{results.ConnectionStatus.HttpStatusCode}\".", results.ConnectionStatus.OriginalException);

            if (IsCacheEnabled)
                await SetCachedQueryResultAsync(query, results.Count, "count").AnyContext();

            return results.Count;
        }

        public async Task<long> CountAsync() {
            return (await Configuration.Client.CountAsync<T>(c => c.Query(q => q.MatchAll()).Indices(GetIndexesByQuery(null))).AnyContext()).Count;
        }

        public async Task<T> GetByIdAsync(string id, bool useCache = false, TimeSpan? expiresIn = null) {
            if (String.IsNullOrEmpty(id))
                return null;

            T result = null;
            if (IsCacheEnabled && useCache)
                result = await Cache.GetAsync<T>(id, null).AnyContext();

            if (result != null)
                return result;

            string index = GetIndexById(id);
            if (!_hasParent) {
                var res = await Configuration.Client.GetAsync<T>(id, index).AnyContext();
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

            if (!_hasIdentity)
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

            if (!_hasParent) {
                itemsToFind.ForEach(id => multiGet.Get<T>(f => f.Id(id).Index(GetIndexById(id))));

                var multiGetResults = await Configuration.Client.MultiGetAsync(multiGet).AnyContext();
                foreach (var doc in multiGetResults.Documents) {
                    if (!doc.Found)
                        continue;

                    results.Documents.Add(doc.Source as T);
                    itemsToFind.Remove(doc.Id);
                }
            }

            // fallback to doing a find
            if (itemsToFind.Count > 0 && (_hasParent || _hasMultipleIndexes))
                results.Documents.AddRange((await FindAsync(new ElasticQuery().WithIds(itemsToFind)).AnyContext()).Documents);

            if (IsCacheEnabled && useCache) {
                foreach (var item in results.Documents)
                    await Cache.SetAsync(((IIdentity)item).Id, item, expiresIn.HasValue ? DateTime.UtcNow.Add(expiresIn.Value) : DateTime.UtcNow.AddSeconds(Configuration.Type.DefaultCacheExpirationSeconds)).AnyContext();
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

        public async Task<ICollection<FacetResult>> GetFacetsAsync(object query) {
            var facetQuery = query as IFacetQuery;

            if (facetQuery == null || facetQuery.FacetFields.Count == 0)
                throw new ArgumentException("Query must contain facet fields.", nameof(query));

            if (Configuration.Type.AllowedFacetFields.Count > 0 && !facetQuery.FacetFields.All(f => Configuration.Type.AllowedFacetFields.Contains(f.Field)))
                throw new ArgumentException("All facet fields must be allowed.", nameof(query));

            var search = CreateSearchDescriptor(query).SearchType(SearchType.Count);
            var res = await Configuration.Client.SearchAsync<T>(search);
            if (!res.IsValid) {
                _logger.Error().Message("Retrieving term stats failed: {0}", res.ServerError.Error).Write();
                throw new ApplicationException("Retrieving term stats failed.");
            }

            return res.ToFacetResults();
        }

        public Task<FindResults<T>> GetBySearchAsync(string systemFilter, string userFilter = null, string query = null, SortingOptions sorting = null, PagingOptions paging = null, FacetOptions facets = null) {
            var search = new ElasticQuery()
                .WithSystemFilter(systemFilter)
                .WithFilter(userFilter)
                .WithSearchQuery(query, false)
                .WithFacets(facets)
                .WithSort(sorting)
                .WithPaging(paging);

            return FindAsync(search);
        }

        public Task<ICollection<FacetResult>> GetFacetsAsync(string systemFilter, FacetOptions facets, string userFilter = null, string query = null) {
            var search = new ElasticQuery()
                .WithSystemFilter(systemFilter)
                .WithFilter(userFilter)
                .WithSearchQuery(query, false)
                .WithFacets(facets);

            return GetFacetsAsync(search);
        }
        
        public bool IsCacheEnabled { get; private set; } = true;
        protected ScopedCacheClient Cache {
            get {
                if (_scopedCacheClient == null) {
                    IsCacheEnabled = Configuration.Cache != null;
                    _scopedCacheClient = new ScopedCacheClient(Configuration.Cache, Configuration.Type.Name);
                }

                return _scopedCacheClient;
            }
        }
        protected void DisableCache() {
            IsCacheEnabled = false;
            _scopedCacheClient = new ScopedCacheClient(new NullCacheClient(), Configuration.Type.Name);
        }
        
        protected virtual async Task InvalidateCacheAsync(ICollection<ModifiedDocument<T>> documents) {
            if (!IsCacheEnabled)
                return;

            if (documents != null && documents.Count > 0 && _hasIdentity) {
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

        protected virtual SearchDescriptor<T> CreateSearchDescriptor(object query) {
            var search = new SearchDescriptor<T>();
            
            search.Query(Configuration.QueryBuilder.CreateQuery<T>(query, Configuration));

            var indices = GetIndexesByQuery(query);
            if (indices?.Length > 0)
                search.Indices(indices);
            search.IgnoreUnavailable();
            
            Configuration.QueryBuilder.BuildSearch(query, Configuration, ref search);

            return search;
        }

        protected string[] GetIndexesByQuery(object query) {
            return !_hasMultipleIndexes ? new[] { Configuration.Index.AliasName } : _timeSeriesIndexType.GetIndexesByQuery(query);
        }

        protected string GetIndexById(string id) {
            return !_hasMultipleIndexes ? Configuration.Index.AliasName : _timeSeriesIndexType.GetIndexById(id);
        }

        protected string GetDocumentIndex(T document) {
            return !_hasMultipleIndexes ? Configuration.Index.AliasName : _timeSeriesIndexType.GetDocumentIndex(document);
        }

        protected Func<T, string> GetParentIdFunc => _hasParent ? d => _childIndexType.GetParentId(d) : (Func<T, string>)null;
        protected Func<T, string> GetDocumentIndexFunc => _hasMultipleIndexes ? d => _timeSeriesIndexType.GetDocumentIndex(d) : (Func<T, string>)(d => Configuration.Index.VersionedName);

        protected async Task<TResult> GetCachedQueryResultAsync<TResult>(object query, string cachePrefix = null, string cacheSuffix = null) {
            var cachedQuery = query as ICachableQuery;
            if (!IsCacheEnabled || cachedQuery == null || !cachedQuery.ShouldUseCache())
                return default(TResult);

            string cacheKey = cachePrefix != null ? cachePrefix + ":" + cachedQuery.CacheKey : cachedQuery.CacheKey;
            cacheKey = cacheSuffix != null ? cacheKey + ":" + cacheSuffix : cacheKey;
            var result = await Cache.GetAsync<TResult>(cacheKey, default(TResult)).AnyContext();
            _logger.Trace().Message("Cache {0}: type={1}", result != null ? "hit" : "miss", Configuration.Type.Name).Write();

            return result;
        }

        protected async Task SetCachedQueryResultAsync<TResult>(object query, TResult result, string cachePrefix = null, string cacheSuffix = null) {
            var cachedQuery = query as ICachableQuery;
            if (!IsCacheEnabled || result == null || cachedQuery == null || !cachedQuery.ShouldUseCache())
                return;

            string cacheKey = cachePrefix != null ? cachePrefix + ":" + cachedQuery.CacheKey : cachedQuery.CacheKey;
            cacheKey = cacheSuffix != null ? cacheKey + ":" + cacheSuffix : cacheKey;
            await Cache.SetAsync(cacheKey, result, cachedQuery.GetCacheExpirationDateUtc()).AnyContext();
        }
    }
}