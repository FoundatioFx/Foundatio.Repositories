using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.Caching;

namespace Foundatio.Repositories.Elasticsearch;

/// <summary>Tracks an explicit write cache key only while its post-write callbacks are running.</summary>
internal sealed class CustomWriteCacheScope : IDisposable
{
    private const int Active = 1;
    private const int Dirty = 2;
    private static readonly AsyncLocal<CustomWriteCacheScope?> _current = new();
    private readonly CustomWriteCacheScope? _parent;
    private readonly ScopedCacheClient _cache;
    private readonly ICacheClient _rootCache;
    private readonly string _prefix;
    private readonly HashSet<string> _writtenIds;
    private int _state = Active;

    public CustomWriteCacheScope(ScopedCacheClient cache, string cacheKey, IEnumerable<string> writtenIds)
    {
        _cache = cache;
        CacheKey = cacheKey;
        (_rootCache, _prefix) = GetCacheIdentity(cache);
        _writtenIds = new HashSet<string>(writtenIds, StringComparer.Ordinal);
        _parent = _current.Value;
        _current.Value = this;
    }

    public string CacheKey { get; }
    public bool IsDirty => (Volatile.Read(ref _state) & Dirty) is not 0;

    public static bool HasActiveScope
    {
        get
        {
            for (var scope = _current.Value; scope is not null; scope = scope._parent)
            {
                if ((Volatile.Read(ref scope._state) & Active) is not 0)
                    return true;
            }

            return false;
        }
    }

    public Task RemoveAsync() => _cache.RemoveAsync(CacheKey);

    public static Task InvalidateAsync(ScopedCacheClient cache, IEnumerable<string> ids)
    {
        var keys = GetMatchingKeys(cache, ids);
        return keys is not null ? cache.RemoveAllAsync(keys) : Task.CompletedTask;
    }

    public static void AppendInvalidationKeys(ScopedCacheClient cache, ICollection<string> keys)
    {
        var matchingKeys = GetMatchingKeys(cache, keys);
        if (matchingKeys is not null)
        {
            foreach (string key in matchingKeys)
                keys.Add(key);
        }
    }

    private static HashSet<string>? GetMatchingKeys(ScopedCacheClient cache, IEnumerable<string> ids)
    {
        if (!HasActiveScope)
            return null;

        var (rootCache, prefix) = GetCacheIdentity(cache);
        HashSet<string>? keys = null;
        foreach (string id in ids)
        {
            for (var scope = _current.Value; scope is not null; scope = scope._parent)
            {
                if (!ReferenceEquals(rootCache, scope._rootCache) || !String.Equals(prefix, scope._prefix, StringComparison.Ordinal)
                    || !scope._writtenIds.Contains(id))
                    continue;

                // Mark only an active scope, atomically with disposal.
                if ((Interlocked.CompareExchange(ref scope._state, Active | Dirty, Active) & Active) is 0)
                    continue;

                (keys ??= new HashSet<string>(StringComparer.Ordinal)).Add(scope.CacheKey);
            }
        }

        return keys;
    }

    private static (ICacheClient Cache, string Prefix) GetCacheIdentity(ICacheClient cache)
    {
        string prefix = String.Empty;
        while (cache is ScopedCacheClient scoped)
        {
            if (scoped.Scope is not null)
                prefix = String.Concat(scoped.Scope, ":", prefix);
            cache = scoped.UnscopedCache;
        }

        return (cache, prefix);
    }

    public void Dispose()
    {
        Interlocked.And(ref _state, ~Active);
        if (ReferenceEquals(_current.Value, this))
            _current.Value = _parent;
    }
}
